#!/usr/bin/env python3

# General python libraries used
import math
import os
import argparse
import csv
from sys import getsizeof

# In order to parse SVS images, we will need to use the OpenSlide library
import openslide

# Pillow (PIL) is being used for the image conversion
# (NOTE) you could install Pillow-SIMD instead of Pillow (see requirements file) for faster processing
import PIL

# This is the clarifai gRPC client
# This is being used to push the tranlated images to Clarifai.com
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2_grpc
from clarifai_grpc.grpc.api import service_pb2, resources_pb2
from clarifai_grpc.grpc.api.status import status_code_pb2

# Enables the use of the Google Protobuf Struct - useful for using the Clarifai.com API
from google.protobuf.struct_pb2 import Struct


# Global Variables
# Create the communication stub here for the Clarifai Client
stub = service_pb2_grpc.V2Stub(ClarifaiChannel.get_grpc_channel())
# Scale factor to decrease the image size - this will decrease the size by dividing height and width by this amount
#       This reduces the image size so the Clarifai system will accept it (25 Mb limit at the current time).
# We need to do this because the images are very large
SCALE_FACTOR = 25

# Helper functions - this is used to encapsulate code so it is easier to read / maintain
def get_project(match):
    '''
    Helper function which extracts out metadata from a known CSV object and returns it.  This is custom for the
    specific CSV that was conveniently available.

    This should be thought of as custom and would likely need to be changed to meet your requirements.

    Returns a dictionary with four different metadata values in it.
    '''
    with open("tcga_metadata.csv", "r") as f:
        csvreader = csv.reader(f, delimiter=",")
        for row in csvreader:
            if row[8] == match:
                return {"primary_site": row[1], "project_disease_type": row[3], "project_name": row[4], "tcga_cancer_type": row[7]}
    return None


def send_image(img_location, meta_data, metadata):
    """
    Sends an image and its metadata to the Clarifai App that was set up (and the key was provided).  This is fairly
    custom for this particular project, but could be adapted to work with other types of projects.
    """

    # Opens the PNG image location passed in.
    with open(img_location, "rb") as f:
        file_bytes = f.read()
    
    #
    # The following set of code generating the metadata to be sent with the API call is somewhat complex.  It is
    # unfortunately using methods included by a common but not universally known library (Protobuf by Google - for 
    # data serialization) as well as dealing with overcomming a current bug in the Clarifai system (currently 
    # being fixed).
    #
    input_metadata = Struct()
    input_metadata.update(meta_data)

    concepts = [resources_pb2.Concept(id=meta_data["general_cancer"], value=1.)]
    project = get_project(meta_data["tcga_id"])

    # This following if statement simply formats the metadata in the way that is desired.  While it looks complex,
    # that is fundementally all that is happening.  The `concepts.append(...)` function is adding each entry of 
    # metadata/    
    if project is not None:
        primary_sites = project["primary_site"].split(";")
        for primary_site in primary_sites:
            primary_site = (primary_site[:31]) if len(primary_site) > 31 else primary_site
            concepts.append(resources_pb2.Concept(id=primary_site.replace(" ", "_"), value=1.))
        project_names = project["project_name"].split(";")
        for project_name in project_names:
            project_name = (project_name[:31]) if len(project_name) > 31 else project_name
            concepts.append(resources_pb2.Concept(id=project_name.replace(" ", "_"), value=1.))
        tcga_cancer_types = project["tcga_cancer_type"].split(";")
        for tcga_cancer_type in tcga_cancer_types:
            tcga_cancer_type = (tcga_cancer_type[:31]) if len(tcga_cancer_type) > 31 else tcga_cancer_type

            concepts.append(resources_pb2.Concept(id=tcga_cancer_type.replace(" ", "_"), value=1.))

    #
    # Call to the Clarifai API.
    # This looks complicated, but this is really almost all boilerplate.  In reality this is just a gRPC call (another
    # commonly used Google library for communication across ports).  In order to understand what is going on here
    # would require a deeper understanding of gRPC works.  However, for these purposes, the image bypes just needs
    # to be put where `base64=` is at and the concepts are included in the `conceps=` line (the concepts we generated 
    # earlier).  We are also adding the raw input metadata we contained in the optional (first) `metadata=` line.
    # Finally, the final `metadata=` line is actually the App access metadata (the API Key from Clarifai) to prove
    # that we have the credentials to access the App.
    #
    post_inputs_response = stub.PostInputs(
        service_pb2.PostInputsRequest(
            inputs=[
                resources_pb2.Input(
                    data=resources_pb2.Data(
                        image=resources_pb2.Image(
                            base64=file_bytes
                        ),
                        concepts=concepts,
                        metadata=input_metadata
                    )
                )
            ]
        ),
        metadata=metadata
    )
    
    #
    # Finally we just check for any type of error and clean up the SVS file to save space.  The removal of the SVS
    # file could be removed if you were going to use them for another purpose.
    #
    if post_inputs_response.status.code != status_code_pb2.SUCCESS:
        print("-- Failed Response: {}".format(post_inputs_response))
        raise Exception("Post inputs failed, status: " + post_inputs_response.status.details)
    else:
        os.remove(img_location)
        svs_img = os.path.splitext(img_location)[0] + '.svs'
        os.remove(svs_img)


def slide_to_scaled_pil_image(slide_path, meta_data):
    """
    Convert a WSI training slide to a scaled-down PIL image.

    Returns:
        Tuple consisting of scaled-down PIL image, original width, original height, new width, and new height.
    """
    # Various print statements are used in place of logging with the Python logging module.
    # This is just for convenience, in a more robust script the logging module should be used.
    print("[ info ] Opening Slide : %s" % slide_path)

    # Attempt to open the slide using OpenSlide (based on the OpenSlide documentation)
    try:
        slide = openslide.open_slide(slide_path)
    except openslide.OpenSlideError:
        slide = None
    except FileNotFoundError:
        slide = None

    # This is just the math to scale the image.  Essentially we need to take the height and the width
    # of a rectangle, and just divide by our scale factor.
    large_w, large_h = slide.dimensions
    new_w = math.floor(large_w / SCALE_FACTOR)
    new_h = math.floor(large_h / SCALE_FACTOR)

    # In the data that we are using, the base level 0 is what we want.  In SVS data there can be
    # a number of other levels having other information, such as thumbnail information or overlayes
    # for the image.  In order to know what to do here you will need to know your data!
    level = 0  #slide.get_best_level_for_downsample(SCALE_FACTOR)

    # Just reading level 0 into a new object.  This is provided by the OpenSlide library and is based
    # on their API.  It it not necessarily code that you would 'just know' without consulting how
    # to use the library!
    whole_slide_image = slide.read_region((0, 0), level, slide.level_dimensions[level])

    # Here we want to convert the image from whatever format it is in internally to RGB (red-green-blue) format.
    # The Clarifai platform can understand several different formats, but RGB is a common one most images you're 
    # used to are in.
    whole_slide_image = whole_slide_image.convert("RGB")

    # Here is the actual resizing of the image.  Note that we are resizing not the original, but the copy that
    # we've made in memory.  Once again, this is a function provided by the OpenSlide API and how to use this
    # is provided through their API instructions.  We are taking advantage of a typical object type in python,
    # a PIL image - while this is not a built in type it is common to use in data science.  Fundementally this
    # is just a NumPy array (something also not built into Python).  You can just think of this as a vector or 
    # matrix of all of the pixel values of the image.
    img = whole_slide_image.resize((new_w, new_h), PIL.Image.BILINEAR)

    # Here we are going to actually save the new file created - the resized SVS file as a PNG file.  This is
    # a typical image type you could open up on your computer without any special software.  The code here is just
    # taking the original file name and replacing the '.svs' extension with '.png' and then saving it along side
    # the SVS version.
    new_slide_name = os.path.splitext(slide_path)[0] + '.png'
    img.save(new_slide_name)

    # We will now return the PNG file name, so we could open it up in the next step.  Alternatively the code could
    # be written to avoid saving and re-opening the file; sending the bytes directly (PIL/NumPy objects).  This would
    # be a little faster, but the overhead in this program comes mostly from opening the SVS image and resizing it
    return new_slide_name


#
# Take in the path for the SVS file (slide) along with the key to use to call Clarifai
#
if __name__ == '__main__':
    # Creating a convenient way to allow for command line arguments
    parser = argparse.ArgumentParser(description='Process SVS slide.')
    parser.add_argument('slide_path', help='SVS file path')
    parser.add_argument('-key', '-k', help='Clarifai.com API key for the App that the images should be posted to.')
    args = parser.parse_args()
    
    # Construct the Clarifai API metadata object, based on the instructions in the API documentation
    api_metadata = (('authorization', 'Key {}'.format(args.key)),)
    
    # Construct the metadata including the cancer type, and the GDC ID, etc.
    # This could really include whatever information is relevant to label the
    # images with.
    #
    # In this particular case, we are just taking advantage of the fact that this
    # information is contained in the paths downloaded, and parsing from there.
    # A more robust implementation would query the metadata of an item based on
    # the GDC ID or some other information extracted from the file name.
    split_vars = args.slide_path.split("/")
    tcga_full_id = split_vars[3].split(".")[0]
    tcga = tcga_full_id.split("-")

    sample_metadata = {
        "general_cancer": split_vars[1],
        "gdc_id": split_vars[2],
        "tcga_full_id": tcga_full_id,
        "tcga_id": tcga[0] + "-" + tcga[1] + "-" + tcga[2]
    }

    # These are the functions that make up the 'pipeline'
    #   1) We need to scale the SVS image to something smaller based on our scale factor.
    #   2) Take that output and send it to the Clarifai platform by using the Clarifai Client
    new_slide_name = slide_to_scaled_pil_image(args.slide_path, sample_metadata)
    send_image(new_slide_name, sample_metadata, api_metadata)



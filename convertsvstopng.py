#!/usr/bin/env python3

import openslide
import math
import PIL
import os
#import multiprocessing
import argparse
import csv
from clarifai_grpc.channel.clarifai_channel import ClarifaiChannel
from clarifai_grpc.grpc.api import service_pb2_grpc
from clarifai_grpc.grpc.api import service_pb2, resources_pb2
from clarifai_grpc.grpc.api.status import status_code_pb2
from google.protobuf.struct_pb2 import Struct
from sys import getsizeof

stub = service_pb2_grpc.V2Stub(ClarifaiChannel.get_grpc_channel())
metadata = (('authorization', 'Key e0e1d8d60b80442790bac3e960f3bc51'),)

SCALE_FACTOR = 25


def get_project(match):
    with open("tcga_metadata.csv", "r") as f:
        csvreader = csv.reader(f, delimiter=",")
        for row in csvreader:
            if row[8] == match:
                return {"primary_site": row[1], "project_disease_type": row[3], "project_name": row[4], "tcga_cancer_type": row[7]}
    return None


def send_image(img_location, meta_data):
    with open(img_location, "rb") as f:
        file_bytes = f.read()
    
    print("Image size: {}".format(getsizeof(file_bytes)))

    input_metadata = Struct()
    input_metadata.update(meta_data)

    concepts = [resources_pb2.Concept(id=meta_data["general_cancer"], value=1.)]

    project = get_project(meta_data["tcga_id"])
    print("Project: {}".format(project))
    # import pdb;pdb.set_trace()
    if project is not None:
        # "primary_site": row[1], "project_disease_type": row[3], "project_name": row[4]}
        primary_sites = project["primary_site"].split(";")
        for primary_site in primary_sites:
            primary_site = (primary_site[:31]) if len(primary_site) > 31 else primary_site
            concepts.append(resources_pb2.Concept(id=primary_site.replace(" ", "_"), value=1.))
        # project_disease_types = project["project_disease_type"].split(";")
        # for project_disease_type in project_disease_types:
        #     project_disease_type = (project_disease_type[:31]) if len(project_disease_type) > 31 else project_disease_type
        #     concepts.append(resources_pb2.Concept(id=project_disease_type.replace(" ", "_"), value=1.))
        project_names = project["project_name"].split(";")
        for project_name in project_names:
            project_name = (project_name[:31]) if len(project_name) > 31 else project_name
            concepts.append(resources_pb2.Concept(id=project_name.replace(" ", "_"), value=1.))
        tcga_cancer_types = project["tcga_cancer_type"].split(";")
        for tcga_cancer_type in tcga_cancer_types:
            tcga_cancer_type = (tcga_cancer_type[:31]) if len(tcga_cancer_type) > 31 else tcga_cancer_type

            concepts.append(resources_pb2.Concept(id=tcga_cancer_type.replace(" ", "_"), value=1.))

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
    
    if post_inputs_response.status.code != status_code_pb2.SUCCESS:
        print("-- Failed Response: {}".format(post_inputs_response))
        raise Exception("Post inputs failed, status: " + post_inputs_response.status.details)
    else:
        os.remove(img_location)
        svs_img = os.path.splitext(img_location)[0] + '.svs'
        os.remove(svs_img)

def open_slide(filename):
    try:
        slide = openslide.open_slide(filename)
    except openslide.OpenSlideError:
        slide = None
    except FileNotFoundError:
        slide = None
    return slide


def slide_to_scaled_pil_image(slide_path, meta_data):
    """
  Convert a WSI training slide to a scaled-down PIL image.

  Returns:
    Tuple consisting of scaled-down PIL image, original width, original height, new width, and new height.
  """
    print("Opening Slide : %s" % slide_path)
    slide = open_slide(slide_path)

    large_w, large_h = slide.dimensions
    new_w = math.floor(large_w / SCALE_FACTOR)
    new_h = math.floor(large_h / SCALE_FACTOR)
    level = 0  #slide.get_best_level_for_downsample(SCALE_FACTOR)
    whole_slide_image = slide.read_region((0, 0), level, slide.level_dimensions[level])
    whole_slide_image = whole_slide_image.convert("RGB")
    # img = whole_slide_image
    # new_w = large_w
    # new_h = large_h
    img = whole_slide_image.resize((new_w, new_h), PIL.Image.BILINEAR)
    new_slide_name = os.path.splitext(slide_path)[0] + '.png'
    print("Saving image to: {}".format(new_slide_name))
    img.save(new_slide_name)

    send_image(new_slide_name, meta_data)
    # return
    # return img, large_w, large_h, new_w, new_h


# def multiprocess_training_slides_to_images():
#     """
#   Convert all WSI training slides to smaller images using multiple processes (one process per core).
#   Each process will process a range of slide numbers.
#   """
#     # how many processes to use
#     num_processes = multiprocessing.cpu_count()
#     pool = multiprocessing.Pool(num_processes)
#
#     num_train_images = get_num_training_slides()
#     if num_processes > num_train_images:
#         num_processes = num_train_images
#     images_per_process = num_train_images / num_processes
#
#     print("Number of processes: " + str(num_processes))
#     print("Number of training images: " + str(num_train_images))
#
#     # each task specifies a range of slides
#     tasks = []
#     for num_process in range(1, num_processes + 1):
#         start_index = (num_process - 1) * images_per_process + 1
#         end_index = num_process * images_per_process
#         start_index = int(start_index)
#         end_index = int(end_index)
#         tasks.append((start_index, end_index))
#         if start_index == end_index:
#             print("Task #" + str(num_process) + ": Process slide " + str(start_index))
#         else:
#             print("Task #" + str(num_process) + ": Process slides " + str(start_index) + " to " + str(end_index))
#
#     # start tasks
#     results = []
#     for t in tasks:
#         results.append(pool.apply_async(training_slide_range_to_images, t))
#
#     for result in results:
#         (start_ind, end_ind) = result.get()
#         if start_ind == end_ind:
#             print("Done converting slide %d" % start_ind)
#         else:
#             print("Done converting slides %d through %d" % (start_ind, end_ind))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Process SVS slide.')
    parser.add_argument('slide_path', help='SVS file path')

    args = parser.parse_args()
    split_vars = args.slide_path.split("/")
    tcga_full_id = split_vars[3].split(".")[0]
    tcga = tcga_full_id.split("-")

    md = {
        "general_cancer": split_vars[1],
        "gdc_id": split_vars[2],
        "tcga_full_id": tcga_full_id,
        "tcga_id": tcga[0] + "-" + tcga[1] + "-" + tcga[2]
    }

    slide_to_scaled_pil_image(args.slide_path, md)



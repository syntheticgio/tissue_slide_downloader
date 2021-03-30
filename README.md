# tissue_slide_downloader
Code to convert SVS files into JPEG and upload to Clarifai for CV purposes.

In order to run this, you should have downloaded a manifest of SVS data from TCGA in a subdirectory (named anything appropriate)
as a classification.  The current TCGA metadata file is hard coded, and may not be sufficient for your purposes and should be changed (along with the code there) if needed.

`python3 convertsvstopng.py <SVS path> <Clarifai API key>`

This should be run from the parent directory (upstream of the subdirectory where the manifest was downloaded).  This is due to the fact that it relies on that directory structure to parse some of the metadata.  That part of the code (in the main() function) could be updated to remove this dependency.

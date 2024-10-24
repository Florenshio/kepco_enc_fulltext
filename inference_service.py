import logging
import os
import t3qai_client as tc
from t3qai_client import T3QAI_MODULE_PATH, T3QAI_INIT_MODEL_PATH
from exe_full_text import *
import ocr
import pytesseract
import threading

logger = logging.getLogger()
logger.setLevel('INFO')

def init_model():
    easyocr = ocr.Inference()
    model_info_dict = {
        "pdf_path" : "",
        "png_lake" : "",
        "easyocr" : easyocr,
        "tesseract" : pytesseract
    }
    return model_info_dict

def inference_dataframe(input_data, model_info_dict):
    """
    input_data : pdf 경로(pdf_path), png 저장소 경로(png_lake)
    """
    # json이 저장될 hdfs의 디렉토리 경로
    json_path = "/tmp_json"

    pdf_path = input_data["pdf_path"]
    png_lake_path = input_data["png_lake"]
    model_info_dict["pdf_path"] = pdf_path
    model_info_dict["png_lake"] = png_lake_path

    argument = (model_info_dict["pdf_path"], model_info_dict["png_lake"], model_info_dict["tesseract"], model_info_dict["easyocr"], json_path)
    thread = threading.Thread(target = execute_fulltext_api, args = argument)
    thread.start()

    return json_path
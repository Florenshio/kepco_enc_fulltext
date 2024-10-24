import os
import fitz
from easyocr import easyocr
from error_status import *
import logging

# ================================================================================================================
class ModelLoader:
    """
    # easyocr 모델 불러오기

    # 20240207 Edit by YOUNGRAE CHO
    """
    def __init__(self):

        # easyocr 모델 객체 선언
        self.easyocr = self.load_easyocr()

    # easyocr 모델 load
    def load_easyocr(self):

        reader = easyocr.Reader(['ko', 'en'], gpu=True)

        return reader
    
# ================================================================================================================
    
class Inference:
    """
    # easyocr 추론

    # 20240214 Edit by YOUNGRAE CHO
    """
    # 인스턴스 생성시 모델 load
    def __init__(self):
        self.reader = ModelLoader().easyocr

    # ocr로 텍스트 추출
    def extract_text(self, image):

        # return할 결과값 형식 지정
        output = {
            "STATUS" : "",
            "STATUS_RESULT" : "",
            "TEXT" : ""
        }

        try:
            result = self.reader.readtext(image)

            # 에러 발생 안했으면 output에 결과 반영
            output["STATUS"] = "200"
            output["STATUS_RESULT"] = "성공"

            # easyocr 결과값에서 텍스트 부분만 정리
            for line in result:
                output["TEXT"] += (" " + line[1])

        except Exception:
            # 에러 발생 시 에러 정보 추출 후 output에 결과 반영
            output = extract_error_status(output)

            return output
        
        return output
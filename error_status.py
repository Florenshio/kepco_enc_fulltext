import traceback
import sys
import fitz
import re

# 에러 코드와 실제 에러 매핑
error_type_dict = {
    "400" : ["fitz.fitz.FileNotFoundError", "FileNotFoundError", "KeyError", "AttributeError", "IndexError"],
    "413" : ["MemoryError", "RuntimeError", "ResourceExhaustedError"],
    "415" : ["fitz.fitz.FileDataError", "FileDataError", "UnicodeDecodeError"],
    "422" : ["SyntaxError"]
}

# ================================================================================================================
def error_tb():
    """
    # 에러 정보 추출

    # 20240215 Edit by YOUNGRAE CHO

    try, except 구문의 Exception 안에서 사용한다.

    해당 함수는 'extract_error_status' 함수에 내장된다.
    """
    # 발생한 예외(Exception) 정보
    exc_type, exc_value, exc_traceback = sys.exc_info()

    # 예외의 traceback 추출
    tb = traceback.extract_tb(exc_traceback)
    filename, line_number, function_name, text = tb[-1]

    # 에러 클래스 이름만을 정규식으로 추출
    error_class_name = re.match(r"<class '(.+)'>", str(exc_type)).group(1)

    # 에러 정보를 dict로 정리 후 return
    error_info = {
        "Error Type" : error_class_name,
        "Error Value" : error_class_name + f": {str(exc_value)}",
        "Error File" : filename,
        "Error Line" : str(line_number),
        "Error Function" : function_name,
        "Error Code" : text
    }

    return error_info

# ================================================================================================================
def extract_error_status(output):
    """
    # 에러 정보 추출

    # 20240219 Edit by YOUNGRAE CHO

    output = {
            "STATUS" : "",
            "STATUS_RESULT" : "",
                    .
                    .
                    .
            }
    """
    # 에러 정보를 추출
    error_info = error_tb()

    # 미리 정의한 에러 코드와 추출된 에러 정보를 대조하여 output 값에 반영한다.
    for key, value in error_type_dict.items():
        for error in value:
            if error_info["Error Type"] == error:
                output["STATUS"] = key
                output["STATUS_RESULT"] = error_info["Error Value"]
    if not output["STATUS"]:
        output["STATUS"] = "500"
        output["STATUS_RESULT"] = f"etc : {error_info["Error Value"]} => file : {error_info["Error File"]} => line : {error_info["Error Line"]}"

    return output
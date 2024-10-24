from full_text import *
from pdf2png import MakePngLake

# Full Text API 실행 함수
def execute_fulltext_api(pdf_path, png_lake, tesseract, easyocr, json_path):
    """
    # Full Text API 실행

    pdf_path : pdf 문서의 경로
    png_lake : pdf 문서의 페이지 별 이미지들이 저장될 directory의 경로
    tesseract : pytesseract model
    easyocr : easyocr model
    json_path : json파일이 저장될 hdfs의 json 저장소의 경로
    """
    # pdf의 페이지 별 이미지 변환
    make_png = MakePngLake(pdf_path, png_lake, tesseract)
    result = make_png.execute_pdf2png_function()

    # 이미지 변환 성공시 결과값은 3개
    if len(result) == 3:
        # 이미지 변환 성공 여부 재확인
        if result[2]["STATUS"] == "200":

            # pdf 문서의 full text 추출
            output = extract_text(pdf_path, result[0], result[1], easyocr, json_path)

            return output
        
        # 만일 이미지 변환이 성공하지 않았을 경우, 해당 에러 정보를 담은 결과값 return
        else:
            return result[2]
        
    # 결과값이 3개가 아니라는 것은 에러가 발생했다는 의미이므로, 에러 정보를 담은 결과값 return
    else:
        return result    
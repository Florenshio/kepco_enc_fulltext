import os
import json
import pandas as pd
import cv2
import numpy as np
from io import BytesIO
from PIL import Image
import re
from erase_table_line import EraseTableLine
from error_status import *
import pyhdfs
import logging

def hdfs_walk(hdfs_client, hdfs_path):
    """
    # 20240305 Edit by YOUNGRAE CHO

    os.walk()와 유사한 기능을 pyhdfs로 구현한 함수
    pyhdfs의 list_status() 함수를 통해 hdfs의 디렉토리 트리를 재귀적으로 순회한다.

    hdfs_client : pyhdfs의 HdfsClient 객체
    hdfs_path : 조회하려는 hdfs 디렉토리 경로
    """
    dirs, files = [], []

    for item in hdfs_client.list_status(hdfs_path):
        if item["type"] == "DIRECTORY":
            dirs.append(item["pathSuffix"])
        elif item["type"] == "FILE":
            files.append(item["pathSuffix"])

    yield hdfs_path, dirs, files

    for dir in dirs:
        new_path = f"{hdfs_path.rstrip('/')}/{dir}"
        yield from hdfs_walk(hdfs_client, new_path)

# ================================================================================================================
def extract_text(pdf_path, file_name, output_directory, easyocr, json_path, hdfs_hosts="hdfs.kdap.svc.cluster.local:9870"):
    """
    # 20240307 Edit by YOUNGRAE CHO

    pdf_path : pdf문서의 경로
    file_name : pdf 파일의 이름
    output_directory : 해당 pdf의 변환된 png 파일들이 저장되는 디렉토리 경로
    easyocr : easyocr model
    json_path : json파일이 저장될 hdfs의 json 저장소의 경로

    Scanned 페이지임을 검증 후, 모든 조건이 일치하지 않으면 Readable 페이지로 간주
    Scanned 페이지면 OCR, Readable 페이지면 라이브러리를 통해 텍스트 추출

    output 값은 json으로 hdfs에 저장된다.
    """

    # hdfs와의 연결을 위한 객체 설정
    fs = pyhdfs.HdfsClient(hosts = hdfs_hosts)
    # hdfs의 pdf 파일을 바이너리 형태로 불러오기
    with fs.open(pdf_path) as fp:
        content = fp.read()

    # 결과값 형식 지정
    output = {
        "STATUS" : "",
        "STATUS_RESULT" : "",
        "PAGE_COUNT" : "",
        "DOC_SEARCHABLE" : "",
        "FULL_TEXT" : "",
        "PAGES" : []
    }

    try:
        # 문서 열기
        doc = fitz.open(stream=content)
        # 문서의 전체 페이지 수 계산
        page_count = doc.page_count
        output["PAGE_COUNT"] = str(page_count)

        # Scanned 문서 여부 확인용 (마지막까지 0이면 Searchable 문서)
        scanned_page_count = 0

        # 각 페이지 별로 텍스트 추출
        for page_num in range(page_count):

            # 페이지 선언
            page = doc[page_num]

            # 페이지의 블록 정보 추출 및 저장
            page_block_info = []
            for idx, block in enumerate(page.get_text_blocks()):
                page_block_info.append(block[4])

            # 페이지 정보, output의 'pages'에 추가
            output["PAGES"].append({
                                    "page_number" : str(page_num + 1),
                                    "page_searchable" : "",
                                    "page_text" : ""
                                    })
            
            # Scanned page 여부 확인용 (마지막까지 0이면 Searchable 페이지)
            scanned_page_count_per_page = 0

            # ==================================== < O C R > ====================================
            # image : DeviceGray 블록 정보 확인
            Devicegray = False
            for block in page_block_info:
                if re.match(r"(<image: DeviceGray).+", block):
                    Devicegray = True

            # image : DeviceGray 블록 정보가 존재하면 Scanned 페이지로 간주 => OCR로 'page_text' 추출
            if Devicegray:

                # 해당 페이지의 searchable 여부
                output["PAGES"][page_num]["page_searchable"] = "False"
                # Scanned image가 하나라도 있으면 Scanned PDF
                output["DOC_SEARCHABLE"] = "False"

                # 변환된 png 경로
                page_path = os.path.join(output_directory, f"page_{str(page_num + 1).zfill(4)}")
                image_path = os.path.join(page_path, f"{file_name}-i{str(page_num + 1).zfill(4)}.png")

                # png 경로를 통해 hdfs에서 이미지 불러오기
                with fs.open(image_path) as byte_image:
                    image_content = byte_image.read()

                # 불러온 이미지를 array로 변환
                image_stream = BytesIO(image_content)
                image_bytes = np.array(bytearray(image_stream.read()), dtype=np.uint8)

                # ======================= 이미지 전처리 =======================

                # 이미지를 opencv로 열기
                cv_image = cv2.imdecode(image_bytes, cv2.IMREAD_COLOR)

                # 이미지에서 표 구분선 삭제
                erased_line_image = EraseTableLine(cv_image).execute_all_erase_function()

                # ======================= 이미지 전처리 끝 =======================

                try:
                    logging.info("Start OCR")
                    # OCR -> 전처리한 이미지 넣기
                    page_ocr_result = easyocr.extract_text(erased_line_image)
                    logging.info("End OCR")

                    # OCR 결과값의 STATUS 여부 확인
                    if page_ocr_result["STATUS"] == "200":
                        
                        # OCR 성공 시 텍스트 추출 성공
                        output["STATUS"] = "200"
                        output["STATUS_RESULT"] = "성공"

                        # result의 문서 전체 텍스트에 추가
                        output["FULL_TEXT"] += page_ocr_result["TEXT"]

                        # result의 페이지 별 텍스트에 추가
                        output["PAGES"][page_num]["page_text"] += page_ocr_result["TEXT"]

                        # Scanned page가 하나 이상 존재하는지 기록
                        scanned_page_count += 1
                        scanned_page_count_per_page += 1

                    else:
                        # OCR 실패 시 텍스트 추출 실패
                        output["STATUS"] = page_ocr_result["STATUS"]
                        output["STATUS_RESULT"] = page_ocr_result["STATUS_RESULT"]

                        return output
                    
                except Exception:
                    # OCR 에러 발생 시 텍스트 추출 실패
                    output = extract_error_status(output)
                    return output
            else:
                pass

            # ==================================== < O C R > ====================================
            # 페이지 블록 정보들 중에서 ICCBased 블록 정보 유무 확인
            Iccbased = False
            for block in page_block_info:
                if re.match(r".+(ICCBased).+", block):
                    Iccbased = True

            # ICCBased 블록 정보 있으면 Readable 텍스트는 추출 후 ICCBased 이미지들에 대해 OCR 실행
            if Iccbased:

                # 해당 페이지의 searchable 여부
                output["PAGES"][page_num]["page_searchable"] = "False"
                # Scanned image가 하나라도 있으면 Scanned PDF
                output["DOC_SEARCHABLE"] = "False"

                # 변환된 png 경로
                page_path = os.path.join(output_directory, f"page_{str(page_num + 1).zfill(4)}")
                image_path = os.path.join(page_path, f"{file_name}-i{str(page_num + 1).zfill(4)}.png")

                # 우선 Readable 텍스트 추출
                output["FULL_TEXT"] += (page.get_text())

                # inner image 경로
                inner_image_dir = page_path + "/inner_images"

                # hdfs의 inner image 디렉토리 순회하면서 inner image 경로 찾기
                for root, directory, files in hdfs_walk(fs, inner_image_dir):

                    # inner image 경로 조합
                    for file in files:
                        inner_image_path = os.path.join(root, file)

                        # inner image 경로를 통해 hdfs에서 이미지 불러오기
                        with fs.open(inner_image_path) as byte_image:
                            image_content = byte_image.read()

                        # 불러온 이미지를 array로 변환
                        image_stream = BytesIO(image_content)
                        image_bytes = np.asarray(bytearray(image_stream.read()), dtype=np.uint8)

                        # ======================= 이미지 전처리 =======================

                        # 이미지를 opencv로 열기
                        cv_image = cv2.imdecode(image_bytes, cv2.IMREAD_COLOR)

                        # 이미지에서 표 구분선 삭제
                        erased_line_image = EraseTableLine(cv_image).execute_all_erase_function()

                        # ======================= 이미지 전처리 끝 =======================

                        try:
                            logging.info("Start OCR")
                            # OCR -> 전처리한 이미지 넣기
                            page_ocr_result = easyocr.extract_text(erased_line_image)
                            logging.info("End OCR")

                            # OCR 결과값의 STATUS 여부 확인
                            if page_ocr_result["STATUS"] == "200":
                                
                                # OCR 성공 시 텍스트 추출 성공
                                output["STATUS"] = "200"
                                output["STATUS_RESULT"] = "성공"

                                # result의 문서 전체 텍스트에 추가
                                output["FULL_TEXT"] += page_ocr_result["TEXT"]

                                # result의 페이지 별 텍스트에 추가
                                output["PAGES"][page_num]["page_text"] += page_ocr_result["TEXT"]

                                # Scanned page가 하나 이상 존재하는지 기록
                                scanned_page_count += 1
                                scanned_page_count_per_page += 1

                            else:
                                # OCR 실패 시 텍스트 추출 실패
                                output["STATUS"] = page_ocr_result["STATUS"]
                                output["STATUS_RESULT"] = page_ocr_result["STATUS_RESULT"]

                                return output
                            
                        except Exception:
                            # OCR 에러 발생 시 텍스트 추출 실패
                            output = extract_error_status(output)
                            return output
                        
            else:
                pass

            # ==================================== < O C R > ====================================
            # 해당 페이지는 존재하지만 어떤 블록 정보도 나오지 않을 경우 Scanned 페이지로 간주
            if not page_block_info:

                # 해당 페이지의 searchable 여부
                output["PAGES"][page_num]["page_searchable"] = "False"
                # Scanned image가 하나라도 있으면 Scanned PDF
                output["DOC_SEARCHABLE"] = "False"

                # 변환된 png 경로
                page_path = os.path.join(output_directory, f"page_{str(page_num + 1).zfill(4)}")
                image_path = os.path.join(page_path, f"{file_name}-i{str(page_num + 1).zfill(4)}.png")

                # png 경로를 통해 hdfs에서 이미지 불러오기
                with fs.open(image_path) as byte_image:
                    image_content = byte_image.read()

                # 불러온 이미지를 array로 변환
                image_stream = BytesIO(image_content)
                image_bytes = np.array(bytearray(image_stream.read()), dtype=np.uint8)

                # ======================= 이미지 전처리 =======================

                # 이미지를 opencv로 열기
                cv_image = cv2.imdecode(image_bytes, cv2.IMREAD_COLOR)

                # 이미지에서 표 구분선 삭제
                erased_line_image = EraseTableLine(cv_image).execute_all_erase_function()

                # ======================= 이미지 전처리 끝 =======================

                try:
                    logging.info("Start OCR")
                    # OCR -> 전처리한 이미지 넣기
                    page_ocr_result = easyocr.extract_text(erased_line_image)
                    logging.info("End OCR")

                    # OCR 결과값의 STATUS 여부 확인
                    if page_ocr_result["STATUS"] == "200":
                        
                        # OCR 성공 시 텍스트 추출 성공
                        output["STATUS"] = "200"
                        output["STATUS_RESULT"] = "성공"

                        # result의 문서 전체 텍스트에 추가
                        output["FULL_TEXT"] += page_ocr_result["TEXT"]

                        # result의 페이지 별 텍스트에 추가
                        output["PAGES"][page_num]["page_text"] += page_ocr_result["TEXT"]

                        # Scanned page가 하나 이상 존재하는지 기록
                        scanned_page_count += 1
                        scanned_page_count_per_page += 1

                    else:
                        # OCR 실패 시 텍스트 추출 실패
                        output["STATUS"] = page_ocr_result["STATUS"]
                        output["STATUS_RESULT"] = page_ocr_result["STATUS_RESULT"]

                        return output
                    
                except Exception:
                    # OCR 에러 발생 시 텍스트 추출 실패
                    output = extract_error_status(output)
                    return output
            
            # ==================================== < R e a d a b l e > ====================================
            # 모든 Scanned 페이지 조건이 일치하지 않으면 Readable 페이지로 간주
            else:
                # Scanned image가 하나도 없으면 Searchable PDF
                if scanned_page_count == 0:

                    output["DOC_SEARCHABLE"] = "True"

                if scanned_page_count_per_page == 0:

                    # 해당 페이지의 searchable 여부
                    output["PAGES"][page_num]["page_searchable"] = "True"

                    try:
                        # 문서 전체 텍스트 result에 추가
                        output["FULL_TEXT"] += page.get_text()

                        # 페이지 별 텍스트 result에 추가
                        output["PAGES"][page_num]["page_text"] += page.get_text()

                        # 텍스트 추출 성공
                        output["STATUS"] = "200"
                        output["STATUS_RESULT"] = "성공"

                    except Exception:
                        # OCR 에러 발생 시 텍스트 추출 실패
                        output = extract_error_status(output)
                        return output
        # 문서 닫기
        doc.close()

    except Exception:
        output = extract_error_status(output)
        return output
    
    logging.info("End Full Text Extracting")

    # output 값을 json 데이터화
    json_data = json.dumps(output)
    # hdfs에 저장될 json 경로 지정
    result_path = os.path.join(json_path, f"{file_name}.json")
    # hdfs에 json 저장
    fs.create(result_path, json_data.encode(), overwrite=True)
    logging.info("Saved json file in hdfs")





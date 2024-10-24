import pdf2image
import os
import io
import cv2
import numpy as np
import re
from PIL import Image
from erase_table_line import EraseTableLine
from error_status import *
import pyhdfs
import logging

class MakePngLake:
    """
    # pdf2png 변환

    # 20240215 Edit by YOUNGRAE CHO
    """
    def __init__(self, pdf_path, png_lake, tesseract, hdfs_hosts="hdfs.kdap.svc.cluster.local:9870"):
        """
        pdf_path : pdf 문서의 경로
        png_lake : pdf 문서의 페이지 별 이미지들이 저장될 directory의 경로
        tesseract : pytesseract model

        인스턴스 생성 후 execute_pdf2png_function 메소드를 실행한다.
        """
        # hdfs와의 통신을 위한 객체 설정
        self.fs = pyhdfs.HdfsClient(hosts=hdfs_hosts)

        self.pdf_path = pdf_path

        # hdfs에 있는 pdf 파일을 바이트 형태로 불러오기
        with self.fs.open(self.pdf_path) as fp:
            self.content = fp.read()

        self.png_lake = png_lake
        self.tesseract = tesseract
        self.pdf_file_name = os.path.splitext(os.path.basename(pdf_path))[0]

    # ================================================================================================================
    """
    # 이미지 방향 수정
    """
    def fix_direction_for_png(self, png_path, pil_image):
        """
        # 20240129 Edit by YOUNGRAE CHO

        png_path : 저장될 이미지의 경로
        pil_image : osd를 적용할 pil 이미지 객체

        이미지의 표를 제거하는 전처리를 한 후, OSD를 통해 문서의 방향을 감지 및 분류한다.

        시계방향으로 회전되어야 할 각도를 리턴한다.

        해당 함수는 'pdf2png' 함수에 내장된다.
        """
        # PIL 이미지를 Numpy array로 변환
        numpy_image = np.array(pil_image)
        # openCV 이미지로 변환
        cv_img = cv2.cvtColor(numpy_image, cv2.COLOR_RGB2BGR)
        # 이미지 전처리
        erased_line_image = EraseTableLine(cv_img).execute_all_erase_function()

        try:
            # OSD
            osd = self.tesseract.image_to_osd(erased_line_image)

            # OSD 결과값에서 필요한 회전 각도 값을 찾는 패턴
            rotation_pattern = re.compile(r"Rotate:\s(\d+)")

            # 필요한 회전 각도 값 찾기
            degree = int(rotation_pattern.findall(osd)[0])
        except Exception as e:
            return e
        
        return degree
    
    # ================================================================================================================
    """
    # 디렉토리 생성
    """
    def make_png_directory(self, doc):
        """
        # 20240206 Edit by YOUNGRAE CHO

        doc : fitz 모듈로 연 pdf 문서

        1. PDF 파일 이름을 딴 폴더 생성
        2. 그 안에 페이지별 폴더 생성
        """
        # 문서 열어서 페이지 개수 정보 획득
        page_count = doc.page_count

        # 페이지 별 디렉토리 경로 모음 리스트
        page_dir_paths = []

        # hdfs에 해당 pdf 파일의 png 디렉토리 생성
        # ex) output_directory = /sdata/gpudir/lake/png/pdf_name
        png_output_directory = os.path.join(self.png_lake, self.pdf_file_name)
        self.fs.mkdirs(png_output_directory)

        # hdfs에 png 디렉토리 안에 페이지 별로 디렉토리 생성
        for page_num in range(page_count):
            png_page_output_directory = os.path.join(png_output_directory, f"page_{str(page_num + 1).zfill(4)}")
            page_dir_paths.append(png_page_output_directory)
            self.fs.mkdirs(png_page_output_directory)

        return page_dir_paths, png_output_directory
    
    # ================================================================================================================
    """
    # png 변환
    """
    def pdf2png(self, page_dir_paths):
        """
        # 20240206 Edit by YOUNGRAE CHO

        page_dir_paths : 문서의 페이지 이미지가 저장되는 디렉토리 경로들의 리스트

        하나의 pdf 파일의 각 페이지를 Tesseract OSD를 이용해 알맞은 각도로 조정한다.

        조정된 페이지를  png로 변환한다.

        원본 이미지의 크기와 해상도 그대로 변환한다.
        """

        # 페이지 별로 png 추출, 저장
        png_images = pdf2image.convert_from_bytes(self.content)

        for page_number, image in enumerate(png_images):
            # 저장될 이미지 경로 지정
            image_path = f"{page_dir_paths[page_number]}/{self.pdf_file_name}-i{str(page_number + 1).zfill(4)}.png"

            # OSD로 회전시킬 각도 구하기
            degree = self.fix_direction_for_png(image_path, image)

            # hdfs 저장을 위해 PIL 이미지를 바이너리 데이터로 변환
            byte_img = io.BytesIO()

            if type(degree) == int:
                # degree 적용해서 이미지 덮어쓰기
                rotate = image.rotate(-degree, expand=True)
                rotate.save(byte_img, format='png')
                byte_img = byte_img.getvalue()
                self.fs.create(image_path, byte_img, overwrite=True)

            # degree 결과 값이 정수가 아닌 경우 osd 에러를 의미 => 원본 이미지를 저장
            else:
                image.save(byte_img, format='png')
                byte_img = byte_img.getvalue()
                self.fs.create(image_path, byte_img, overwrite=True)

    # ================================================================================================================
    """
    # 삽입된 이미지 추출
    """
    def make_inner_image_directory(self, page_dir_paths, page_number):
        """
        # 20240130 Edit by YOUNGRAE CHO

        page_dir_paths : 문서의 페이지 이미지가 저장되는 디렉토리 경로들의 리스트
        page_number : 이미지가 존재하는 페이지 숫자

        pdf 페이지에 삽입된 이미지를 추출해 저장할 디렉토리를 생성한다. 단, 이미지가 존재하는 페이지만 page_number 인자값을 받아서 생성한다.

        해당 함수는 'extract_inner_image_per_one_pdf' 함수에 내장한다.
        """
        # 문서의 페이지 별로 삽입 이미지가 저장될 디렉토리 생성
        inner_image_output_directory = f"{page_dir_paths[page_number]}/inner_images"

        self.fs.mkdirs(inner_image_output_directory)

        return inner_image_output_directory
    
    # ================================================================================================================
    """
    # 삽입된 이미지 추출
    """
    def extract_inner_image_per_one_pdf(self, doc, page_dir_paths):
        """
        # 20240206 Edit by YOUNGRAE CHO

        doc : fitz 모듈로 연 pdf 문서
        page_dir_paths : 문서의 페이지 이미지가 저장되는 디렉토리 경로들의 리스트

        각 페이지에서 삽입된 이미지를 추출한다.

        추출된 이미지를 알맞은 경로와 이름을 지정하여 저장한다.

        해당 함수는 'execute_png_function' 함수에 내장된다.
        """
        # 문서 하나의 모든 추출된 이미지가 저장될 리스트
        page_images_list = []

        # 페이지 별로 이미지 추출
        for page_number, page in enumerate(doc):
            page_images = [io.BytesIO(doc.extract_image(i[0])["image"]) for i in doc.get_page_images(page_number, full=True)]
            page_images_list.append(page_images)

        # 삽입 이미지가 저장될 디렉토리 생성 후 이미지 저장
        for page_number, page_images in enumerate(page_images_list):
            if page_images:
                for idx, image in enumerate(page_images):
                    inner_image_output_directory = self.make_inner_image_directory(page_dir_paths, page_number)
                    image_path = f"{inner_image_output_directory}/image-{str(idx + 1).zfill(3)}.png"

                    self.fs.create(image_path, image, overwrite=True)

    # ================================================================================================================
    # png lake에 각 pdf의 새 디렉토리 생성 후 png 변환
    def execute_pdf2png_function(self):
        """
        # 20240206 Edit by YOUNGRAE CHO

        하나의 pdf 문서를 대상으로 한다.

        1. PNG 디렉토리 생성
        2. 페이지 별로 png 이미지 변환
        3. 페이지 별로 삽입 이미지 추출 및 저장
        """

        output = {
            "STATUS" : "",
            "STATUS_RESULT" : ""
        }

        try:
            doc = fitz.open(stream=self.content)
            # 디렉토리 생성
            page_dir_paths, png_output_directory = self.make_png_directory(doc)
            logging.info("maked png directory : {}".format(png_output_directory))

            # 페이지 별 png 변환
            self.pdf2png(page_dir_paths)
            logging.info("saved png")

            # inner image 추출
            self.extract_inner_image_per_one_pdf(doc, page_dir_paths)
            logging.info("saved inner_images")

            # 문서 닫기
            doc.close()

            output["STATUS"] = "200"
            output["STATUS_RESULT"] = "성공"

        except Exception:
            # 에러 발생 시 에러 정보 추출
            output = extract_error_status(output)

            return output
        
        return self.pdf_file_name, png_output_directory, output
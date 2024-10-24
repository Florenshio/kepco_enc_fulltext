import cv2
import numpy as np

class EraseTableLine:
    """
    # 이미지 전처리

    # 20240207 Edit by YOUNGRAE CHO
    """
    def __init__(self, image):
        self.image = image
    
    # ================================================================================================================
    def detect_contour(self):
        """
        # 20240123 Edit by YOUNGRAE CHO
        
        이미지의 윤곽을 찾아내서 표에 대한 경계를 찾아내기
        
        찾아낸 경계를 검은색(b, g, r = 0)이미지에 흰색(b, g, r = 255)의 사각형을 그려 line만 있는 line_image를 만든다.
        """
        # 원본 이미지의 높이, 넓이
        origin_height, origin_width, _ = self.image.shape
        # 검은색 이미지 생성
        line_image = self.image*0

        # 원본 이미지를 그레이 이미지로 변환
        imgray = cv2.cvtColor(self.image, cv2.COLOR_BGR2GRAY)

        # 이미지에 적응형 임계값 처리를 적용해 이진화
        thr = cv2.adaptiveThreshold(imgray, 255, cv2.ADAPTIVE_THRESH_MEAN_C, cv2.THRESH_BINARY, 11, 2)
        # erode를 이용해 하얀 배경 위의 까만 선을 선명하게
        kernel = np.ones((2, 2), np.uint8)
        thr = cv2.erode(thr, kernel, iterations = 2)

        min_width = 30  # Minimum contour rectangle size
        min_height = 30  # Minimum contour rectangle size
        retrieve_mode = 2  # RETR_EXTRNAL = 0, RETR_LIST = 1, RETR_CCOMP = 2, RETR_TREE = 3, RETR_FLOODFILL = 4
        approx_method = 2  # CHAIN_APPROX_NONE = 1, CHAIN_APPROX_SIMPLE = 2, CHAIN_APPROX_TC89_KCOS = 4, CHAIN_APPROX_TC89_L1 = 3
        contours, hierarchy = cv2.findContours(thr, retrieve_mode, approx_method)


        i = 0
        # 하얀색으로 구분선 그리기
        for contour in contours:
            x, y, width, height = cv2.boundingRect(contour)

            if width > origin_width * 0.5 or height > origin_height * 0.5:
                line_image = cv2.rectangle(line_image, (x, y), (x+width, y+height), (255, 255, 255), 2)

            if (width > min_width and height > min_height) or (hierarchy[0, i, 2] != -1):
                line_image = cv2.rectangle(line_image, (x, y), (x + width, y + height), (255, 255, 255), 2)

            if (width > min_width or height > min_height) and (hierarchy[0, i, 2] == -1):
                line_image = cv2.rectangle(line_image, (x, y), (x + width, y + height), (255, 255, 255), 2)

            i += 1

        return line_image

    # ================================================================================================================
    def morph_closing(self, line_image):
        """
        # 20240123 Edit by YOUNGRAE CHO

        line_image에서 line과 line 사이에 존재하는 공간을 morph close 기법으로 메꾸기

        line과 line 사이에 존재하는 공간은 실제 라인이 존재하는 공간이므로 cell들을 계산할 때 불필요
        """
        # morph close 적용할 커널 설정
        kernel_row = 3
        kernel_col = 3
        kernel = np.ones((kernel_row, kernel_col), np.uint8)

        closing_iter = 2

        # 빈 공간 메우기
        closing_line = cv2.morphologyEX(line_image, cv2.MORPH_CLOSE, kernel, iterations = closing_iter)

        return closing_iter
    
    # ================================================================================================================
    def erase_line(self, closing_line):
        """
        # 20240123 Edit by YOUNGRAE CHO

        흰색의 line_image를 원본 이미지에 덮어 씌워서 표의 구분선을 지운다.
        """
        erased_line = cv2.addWeighted(self.image, 1, closing_line, 1, 0)

        # ocr 성능을 위한 전처리 추가
        gray_erased_line = cv2.cvtColor(erased_line, cv2.COLOR_BGR2GRAY)
        kernel = np.ones((2, 2), np.uint8)
        erode_erased_line = cv2.erode(gray_erased_line, kernel, iterations = 1)

        return erode_erased_line
    
    # ================================================================================================================
    def execute_all_erase_function(self):
        """
        # 20240123 Edit by YOUNGRAE CHO

        이미지 전처리 함수들을 차례대로 실행한다.
        """
        line_image = self.detect_contour()
        closing_line = self.morph_closing(line_image)
        erased_line = self.erase_line(closing_line)

        return erased_line
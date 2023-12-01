from typing import List, Optional

import cv2
import numpy as np
from cv2 import dnn_Net
from genie_common.typing import Image
from genie_datastores.postgres.models import Gender
from numpy import ndarray

from data_collectors.consts.image_gender_detector_consts import MODEL_MEAN_VALUES, FACE_MODEL_PATH, \
    FACE_MODEL_WEIGHTS_PATH, GENDER_MODEL_PATH, GENDER_MODEL_WEIGHTS_PATH, LABELS
from data_collectors.logic.models import ArtistGender


class ImageGenderDetector:
    """ Source: https://www.thepythoncode.com/article/gender-detection-using-opencv-in-python """
    def __init__(self,
                 face_model: dnn_Net,
                 gender_model: dnn_Net,
                 confidence_threshold: float):
        self._face_model = face_model
        self._gender_model = gender_model
        self._confidence_threshold = confidence_threshold

    @classmethod
    def create(cls, confidence_threshold: float) -> "ImageGenderDetector":
        return cls(
            face_model=cls._load_model(weights_path=FACE_MODEL_WEIGHTS_PATH, model_path=FACE_MODEL_PATH),
            gender_model=cls._load_model(weights_path=GENDER_MODEL_WEIGHTS_PATH, model_path=GENDER_MODEL_PATH),
            confidence_threshold=confidence_threshold
        )

    def detect_gender(self, image: ndarray, frame_width: int = 640) -> List[ArtistGender]:
        frame = image.copy()

        if frame.shape[1] > frame_width:
            frame = self._resize_image(frame, width=frame_width)

        faces = self._get_faces(frame)
        return [self._detect_single_face_gender(frame, face) for face in faces]

    @staticmethod
    def _resize_image(image: ndarray, width=None, height=None, inter: int = cv2.INTER_AREA) -> ndarray:
        (h, w) = image.shape[:2]

        if width is None:
            if height is None:
                return image

            r = height / float(h)
            dim = (int(w * r), height)

        else:
            r = width / float(w)
            dim = (width, int(h * r))

        return cv2.resize(image, dim, interpolation=inter)

    def _get_faces(self, frame: ndarray) -> List[Image]:
        try:
            blob = cv2.dnn.blobFromImage(frame, 1.0, (300, 300), (104, 177.0, 123.0))
        except cv2.error:
            return []

        self._face_model.setInput(blob)
        output = np.squeeze(self._face_model.forward())
        faces = []

        for i in range(output.shape[0]):
            face = self._get_single_face(output, frame, i)

            if face is not None:
                faces.append(face)

        return faces

    def _get_single_face(self, output, frame, i) -> Optional[Image]:
        confidence = output[i, 2]

        if confidence <= self._confidence_threshold:
            return

        shape_array = np.array([frame.shape[1], frame.shape[0], frame.shape[1], frame.shape[0]])
        box = output[i, 3:7] * shape_array
        start_x, start_y, end_x, end_y = box.astype(np.int_)
        start_x, start_y, end_x, end_y = start_x - 10, start_y - 10, end_x + 10, end_y + 10
        start_x = 0 if start_x < 0 else start_x
        start_y = 0 if start_y < 0 else start_y
        end_x = 0 if end_x < 0 else end_x
        end_y = 0 if end_y < 0 else end_y

        return start_x, start_y, end_x, end_y

    def _detect_single_face_gender(self, frame: ndarray, face: Image) -> ArtistGender:
        start_x, start_y, end_x, end_y = face
        face_img = frame[start_y: end_y, start_x: end_x]
        blob = cv2.dnn.blobFromImage(
            image=face_img, scalefactor=1.0, size=(227, 227), mean=MODEL_MEAN_VALUES, swapRB=False, crop=False
        )
        self._gender_model.setInput(blob)
        predictions = self._gender_model.forward()
        predicted_gender_index = predictions[0].argmax()
        predicted_gender = LABELS[predicted_gender_index]

        return ArtistGender(
            gender=Gender(predicted_gender),
            confidence=predictions[0][predicted_gender_index]
        )

    @staticmethod
    def _load_model(weights_path: str, model_path: str) -> dnn_Net:
        return cv2.dnn.readNetFromCaffe(weights_path, model_path)

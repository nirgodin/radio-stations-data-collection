from genie_datastores.postgres.models import Gender

MODEL_MEAN_VALUES = (78.4263377603, 87.7689143744, 114.895847746)
LABELS = [Gender.MALE.value, Gender.FEMALE.value]
GENDER_MODEL_RESOURCES_DIR = r"gender_model_resources"
FACE_MODEL_PATH = (
    rf"{GENDER_MODEL_RESOURCES_DIR}/res10_300x300_ssd_iter_140000_fp16.caffemodel"
)
FACE_MODEL_WEIGHTS_PATH = rf"{GENDER_MODEL_RESOURCES_DIR}/deploy.prototxt.txt"
GENDER_MODEL_WEIGHTS_PATH = rf"{GENDER_MODEL_RESOURCES_DIR}/deploy_gender.prototxt"
GENDER_MODEL_PATH = rf"{GENDER_MODEL_RESOURCES_DIR}/gender_net.caffemodel"

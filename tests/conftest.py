import pytest

from bronze.config import Config
from tests.datos_sinteticos import generar


@pytest.fixture
def entorno(tmp_path):
    esperado = generar(tmp_path / "datos_red")
    cfg = Config(data_dir=tmp_path / "datos_red", lake_dir=tmp_path / "lake",
                 kafka_particiones=3, microlote_filas=400, microlote_segundos=5.0, chunk_filas=1000)
    return cfg, esperado

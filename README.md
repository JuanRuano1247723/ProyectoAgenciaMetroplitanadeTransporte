### Para ejecución de Notebook de exploración de archivos
```
python -m prof1cieciadedatos venv #crear entorno virtual

.prof1cieciadedatos\Scripts\Activate #ejecutar entorno virtual windows

source .prof1cieciadedatos/bin/activate #ejecutar entorno virtual macOS 

```
Instalar dependencias

```
pip install pandas matplotlib missingno jupyter ipykernel
python -m ipykernel install --user --name=.prof1cieciadedatos --display-name "Python (.venv)"
```
para ejecutar el notebook
```
jupyter notebook
```


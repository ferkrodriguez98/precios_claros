# Analisis Precios Claros

## Descomprimir todos los zip

Para que el script funcione correctamente, la carpeta `crudo` (donde sea que esté) debe tener la siguiente estructura:

```
crudo/
├── 2020/
│   ├── archivo1.rar
│   ├── archivo2.dta
│   └── ...
├── 2021/
│   ├── archivo3.gz
│   └── ...
└── 2025/
    └── ...
```

Es decir, dentro de `crudo` deben estar todas las carpetas y archivos originales, tal como se recibieron (comprimidos y no comprimidos).

Al ejecutar el script, se generará la carpeta `data` con la misma estructura, pero solo con los archivos descomprimidos y los archivos no comprimidos copiados. 
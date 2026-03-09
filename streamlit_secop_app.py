import ast
import base64
import io
import re
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import streamlit as st


OUTPUT_DIR = Path("output")
ZIP_NAME = "output.zip"


def extraer_urls_desde_excel(excel_file) -> List[str]:
    """Extrae URLs válidas desde cualquier columna del Excel."""
    filename = (getattr(excel_file, "name", "") or "").lower()

    try:
        if filename.endswith(".xls"):
            df = pd.read_excel(excel_file, engine="xlrd")
        else:
            df = pd.read_excel(excel_file, engine="openpyxl")
        values = [value for _, row in df.iterrows() for value in row.tolist()]
    except ImportError as exc:
        if filename.endswith(".xlsx"):
            values = _leer_xlsx_sin_openpyxl(excel_file)
        else:
            dependencia = "xlrd"
            extension = ".xls"
            raise RuntimeError(
                f"No se encontró la dependencia opcional '{dependencia}', necesaria para leer archivos Excel ({extension}). "
                f"Instálala en el entorno con: pip install {dependencia}"
            ) from exc
    except ValueError as exc:
        raise RuntimeError(
            "No fue posible leer el Excel. Verifica que el archivo tenga un formato válido "
            "(.xlsx con openpyxl o .xls con xlrd)."
        ) from exc

    return _extraer_urls_desde_valores(values)


def _leer_xlsx_sin_openpyxl(excel_file) -> List[str]:
    """Fallback para leer texto de `.xlsx` usando solo librerías estándar."""
    if hasattr(excel_file, "getvalue"):
        raw_bytes = excel_file.getvalue()
    else:
        raw_bytes = excel_file.read()

    try:
        with zipfile.ZipFile(io.BytesIO(raw_bytes), "r") as xlsx_zip:
            shared_strings = _leer_shared_strings(xlsx_zip)
            values: List[str] = []

            for sheet_name in sorted(
                name
                for name in xlsx_zip.namelist()
                if name.startswith("xl/worksheets/") and name.endswith(".xml")
            ):
                with xlsx_zip.open(sheet_name) as sheet_file:
                    tree = ET.parse(sheet_file)
                    root = tree.getroot()

                    for cell in root.findall(".//{*}c"):
                        cell_type = cell.attrib.get("t")

                        if cell_type == "inlineStr":
                            inline_text = "".join(cell.itertext()).strip()
                            if inline_text:
                                values.append(inline_text)
                            continue

                        value_node = cell.find("{*}v")
                        if value_node is None or value_node.text is None:
                            continue

                        cell_value = value_node.text.strip()
                        if not cell_value:
                            continue

                        if cell_type == "s":
                            if cell_value.isdigit():
                                index = int(cell_value)
                                if 0 <= index < len(shared_strings):
                                    values.append(shared_strings[index])
                        else:
                            values.append(cell_value)

            return values
    except (zipfile.BadZipFile, ET.ParseError, KeyError, OSError) as exc:
        raise RuntimeError(
            "No fue posible leer el archivo `.xlsx` sin `openpyxl`. "
            "Instala `openpyxl` con: pip install openpyxl"
        ) from exc


def _leer_shared_strings(xlsx_zip: zipfile.ZipFile) -> List[str]:
    try:
        with xlsx_zip.open("xl/sharedStrings.xml") as shared:
            tree = ET.parse(shared)
            root = tree.getroot()
            return ["".join(node.itertext()).strip() for node in root.findall(".//{*}si")]
    except KeyError:
        return []


def _extraer_urls_desde_valores(values: Iterable[object]) -> List[str]:
    urls: List[str] = []

    for value in values:
        if pd.isna(value):
            continue

        text = str(value).strip()
        if not text:
            continue

        # Caso 1: celda con diccionario serializado, ej: {'url': 'https://...'}
        if text.startswith("{") and "url" in text:
            try:
                data = ast.literal_eval(text)
                if isinstance(data, dict) and data.get("url"):
                    urls.append(str(data["url"]).strip())
                    continue
            except (ValueError, SyntaxError):
                pass

        # Caso 2: URL directa o incrustada en texto
        match = re.search(r"https?://[^\s'\"}]+", text)
        if match:
            urls.append(match.group(0))

    # Eliminar duplicados preservando orden
    unique_urls = list(dict.fromkeys(urls))
    return unique_urls


def guardar_paginas_como_pdf(urls: List[str], progreso_placeholder) -> List[Path]:
    """
    Recorre cada URL y guarda un PDF.
    Nota: los captcha sofisticados pueden requerir resolución manual.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_paths: List[Path] = []

    try:
        from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
        from playwright.sync_api import sync_playwright
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "No se encontró la dependencia opcional 'playwright'. "
            "Instálala en el entorno con: pip install playwright && playwright install chromium"
        ) from exc

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context()

        for idx, url in enumerate(urls, start=1):
            progreso_placeholder.info(f"Procesando {idx}/{len(urls)}: {url}")
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=120000)

            # Espera adicional para carga dinámica y posible captcha.
            # Si hay captcha, el usuario puede resolverlo en la ventana abierta.
            tiempo_espera = st.session_state.get("espera_captcha", 30)
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeoutError:
                pass

            page.wait_for_timeout(tiempo_espera * 1000)

            pdf_name = f"secop_{idx:03d}.pdf"
            pdf_path = OUTPUT_DIR / pdf_name
            page.pdf(path=str(pdf_path), format="A4", print_background=True)
            pdf_paths.append(pdf_path)
            page.close()

        browser.close()

    return pdf_paths


def crear_zip(paths: List[Path]) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for p in paths:
            zf.write(p, arcname=p.name)
    buffer.seek(0)
    return buffer.read()


def auto_download_zip(zip_bytes: bytes, filename: str) -> None:
    b64 = base64.b64encode(zip_bytes).decode()
    href = f"data:application/zip;base64,{b64}"
    st.markdown(
        f"""
        <a id="auto-download" href="{href}" download="{filename}"></a>
        <script>
            const a = document.getElementById('auto-download');
            if (a) {{ a.click(); }}
        </script>
        """,
        unsafe_allow_html=True,
    )


def main():
    st.set_page_config(page_title="SECOP a PDF", layout="wide")
    st.title("Automatizador SECOP → PDF + ZIP")

    st.write(
        "Carga tu Excel con URLs, procesa cada enlace y descarga un ZIP con los PDFs generados."
    )

    try:
        import playwright  # noqa: F401
    except ModuleNotFoundError:
        st.warning(
            "Falta la librería `playwright` en este entorno. "
            "Para habilitar el procesamiento instala: "
            "`pip install playwright && playwright install chromium`."
        )

    try:
        import openpyxl  # noqa: F401
    except ModuleNotFoundError:
        st.warning(
            "Falta la librería `openpyxl` en este entorno para leer archivos `.xlsx`. "
            "Instálala con: `pip install openpyxl`."
        )

    st.session_state.setdefault("espera_captcha", 30)
    st.session_state["espera_captcha"] = st.slider(
        "Segundos de espera por URL (útil para resolver captcha/manual)",
        min_value=5,
        max_value=120,
        value=st.session_state["espera_captcha"],
    )

    excel_file = st.file_uploader(
        "Sube el archivo Excel (ej: URL_SECOP.xlsx)",
        type=["xlsx", "xls"],
    )

    if excel_file is None:
        st.info("Esperando archivo Excel.")
        return

    try:
        urls = extraer_urls_desde_excel(excel_file)
    except RuntimeError as exc:
        st.error(str(exc))
        return

    if not urls:
        st.error("No se detectaron URLs válidas en el archivo.")
        return

    st.success(f"Se detectaron {len(urls)} URL(s).")
    st.dataframe(pd.DataFrame({"url": urls}), use_container_width=True)

    if st.button("Procesar URLs y generar PDFs", type="primary"):
        progreso = st.empty()
        inicio = time.time()

        try:
            pdf_paths = guardar_paginas_como_pdf(urls, progreso)
        except RuntimeError as exc:
            st.error(str(exc))
            return
        except Exception as exc:
            st.error(f"Error inesperado al generar PDFs: {exc}")
            return

        zip_bytes = crear_zip(pdf_paths)
        elapsed = time.time() - inicio

        with open(ZIP_NAME, "wb") as f:
            f.write(zip_bytes)

        progreso.success(
            f"Completado: {len(pdf_paths)} PDF(s) en {elapsed:.1f}s. ZIP listo: {ZIP_NAME}"
        )

        st.download_button(
            "Descargar output.zip",
            data=zip_bytes,
            file_name=ZIP_NAME,
            mime="application/zip",
        )

        st.caption(
            "Intentando descarga automática del ZIP en tu carpeta de descargas del navegador..."
        )
        auto_download_zip(zip_bytes, ZIP_NAME)


if __name__ == "__main__":
    main()

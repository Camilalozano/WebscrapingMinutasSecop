import ast
import base64
import io
import re
import time
import zipfile
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


OUTPUT_DIR = Path("output")
ZIP_NAME = "output.zip"


def extraer_urls_desde_excel(excel_file) -> List[str]:
    """Extrae URLs válidas desde cualquier columna del Excel."""
    df = pd.read_excel(excel_file)
    urls: List[str] = []

    for _, row in df.iterrows():
        for value in row.tolist():
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

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
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

    urls = extraer_urls_desde_excel(excel_file)
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
        except Exception as e:
            st.exception(e)
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

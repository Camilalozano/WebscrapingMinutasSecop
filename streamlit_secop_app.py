import ast
import base64
import os
import importlib
import importlib.util
import io
import re
import subprocess
import sys
import time
import zipfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Iterable, List

import pandas as pd
import streamlit as st


OUTPUT_DIR = Path("output")
ZIP_NAME = "output.zip"


def _playwright_disponible() -> bool:
    playwright_spec = importlib.util.find_spec("playwright")
    if playwright_spec is None:
        return False

    return importlib.util.find_spec("playwright.sync_api") is not None


def _openpyxl_disponible() -> bool:
    return importlib.util.find_spec("openpyxl") is not None


def _instalar_chromium_playwright() -> None:
    """Instala el binario de Chromium usado por Playwright si no existe."""
    install_cmds = [
        [sys.executable, "-m", "playwright", "install", "--with-deps", "chromium"],
        [sys.executable, "-m", "playwright", "install", "chromium"],
    ]

    errores = []
    for install_cmd in install_cmds:
        try:
            subprocess.run(install_cmd, check=True, capture_output=True, text=True)
            return
        except subprocess.CalledProcessError as exc:
            stderr = (exc.stderr or "").strip()
            stdout = (exc.stdout or "").strip()
            errores.append(stderr or stdout or str(exc))

    detalle = " | ".join(errores)
    raise RuntimeError(
        "Playwright está instalado, pero faltan los navegadores/dependencias del sistema. "
        "Intenté instalar Chromium automáticamente y falló. "
        f"Detalle: {detalle}"
    )


def _es_error_dependencia_sistema_playwright(exc: Exception) -> bool:
    error_text = str(exc).lower()
    marcadores = [
        "error while loading shared libraries",
        "cannot open shared object file",
        "failed to launch browser process",
        "libgbm.so",
        "libglib-2.0.so",
    ]
    return any(marker in error_text for marker in marcadores)


def _mensaje_dependencias_sistema_playwright(exc: Exception) -> str:
    return (
        "Playwright logró descargar Chromium, pero faltan dependencias nativas del sistema "
        "(por ejemplo `libgbm.so.1` o `libglib-2.0.so.0`). Ejecuta en el servidor: "
        "`python -m playwright install --with-deps chromium` "
        "o instala las librerías faltantes vía apt/yum según tu sistema "
        "(en Streamlit Cloud, usa `packages.txt`). "
        f"Detalle original: {exc}"
    )


def _entorno_tiene_pantalla() -> bool:
    """Indica si el entorno tiene servidor gráfico disponible (X11/Wayland)."""
    return bool(os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY"))


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


def _esperar_resolucion_captcha(page, timeout_s: int) -> bool:
    """Espera a que desaparezca/sea resuelto el reCAPTCHA cuando está presente."""
    inicio = time.time()

    while time.time() - inicio < timeout_s:
        recaptcha_iframes = page.locator(
            "iframe[src*='recaptcha'], iframe[title*='reCAPTCHA']"
        ).count()

        if recaptcha_iframes == 0:
            return True

        token_locator = page.locator("textarea[name='g-recaptcha-response']")
        if token_locator.count() > 0:
            try:
                token = (token_locator.first.input_value() or "").strip()
            except Exception:
                token = (
                    token_locator.first.evaluate("el => (el && el.value) ? el.value : ''")
                    or ""
                ).strip()

            if token:
                return True

        page.wait_for_timeout(1000)

    return False


def guardar_paginas_como_pdf(
    urls: List[str],
    progreso_placeholder,
    espera_captcha_s: int,
    modo_manual_captcha: bool,
    max_espera_resolucion_s: int,
) -> List[Path]:
    """
    Recorre cada URL y guarda un PDF.
    Nota: los captcha sofisticados pueden requerir resolución manual.
    """
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    pdf_paths: List[Path] = []

    if not _playwright_disponible():
        raise RuntimeError(
            "No se encontró la dependencia opcional 'playwright'. "
            "Instálala en el entorno con: pip install playwright && playwright install chromium"
        )

    playwright_sync_api = importlib.import_module("playwright.sync_api")
    PlaywrightTimeoutError = playwright_sync_api.TimeoutError
    sync_playwright = playwright_sync_api.sync_playwright

    headless = not modo_manual_captcha
    if modo_manual_captcha and not _entorno_tiene_pantalla():
        progreso_placeholder.warning(
            "El modo manual de reCAPTCHA requiere interfaz gráfica, pero este servidor no "
            "tiene DISPLAY/WAYLAND. Se usará modo headless automáticamente."
        )
        headless = True

    with sync_playwright() as p:
        try:
            browser = p.chromium.launch(headless=headless)
        except Exception as exc:
            if _es_error_dependencia_sistema_playwright(exc):
                raise RuntimeError(_mensaje_dependencias_sistema_playwright(exc)) from exc

            if not headless and "xserver" in str(exc).lower():
                progreso_placeholder.warning(
                    "No fue posible abrir Chromium con interfaz gráfica (XServer). "
                    "Reintentando en modo headless..."
                )
                browser = p.chromium.launch(headless=True)
            else:
                if "Executable doesn't exist" not in str(exc):
                    raise

                progreso_placeholder.info(
                    "Playwright no encontró Chromium. Instalando navegador automáticamente..."
                )
                _instalar_chromium_playwright()
                try:
                    browser = p.chromium.launch(headless=headless)
                except Exception as launch_exc:
                    if _es_error_dependencia_sistema_playwright(launch_exc):
                        raise RuntimeError(
                            _mensaje_dependencias_sistema_playwright(launch_exc)
                        ) from launch_exc
                    if not headless and "xserver" in str(launch_exc).lower():
                        progreso_placeholder.warning(
                            "Chromium no pudo iniciarse en modo visual tras la instalación. "
                            "Reintentando en modo headless..."
                        )
                        browser = p.chromium.launch(headless=True)
                    else:
                        raise

        context = browser.new_context()

        for idx, url in enumerate(urls, start=1):
            progreso_placeholder.info(f"Procesando {idx}/{len(urls)}: {url}")
            page = context.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=120000)

            # Espera adicional para carga dinámica y posible captcha.
            try:
                page.wait_for_load_state("networkidle", timeout=15000)
            except PlaywrightTimeoutError:
                pass

            if modo_manual_captcha:
                page.bring_to_front()
                progreso_placeholder.warning(
                    "Si aparece el reCAPTCHA, resuélvelo en la ventana del navegador que se abrió. "
                    f"Esperando hasta {max_espera_resolucion_s}s..."
                )
                captcha_resuelto = _esperar_resolucion_captcha(
                    page, max_espera_resolucion_s
                )
                if not captcha_resuelto:
                    progreso_placeholder.warning(
                        "No se detectó la resolución del reCAPTCHA en el tiempo configurado. "
                        "El PDF podría guardarse solo con la pantalla del captcha. "
                        "Aumenta el tiempo de espera o vuelve a intentar resolviéndolo antes."
                    )

            page.wait_for_timeout(espera_captcha_s * 1000)

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

    playwright_instalado = _playwright_disponible()
    if not playwright_instalado:
        st.warning(
            "Falta la librería `playwright` en este entorno. "
            "Para habilitar el procesamiento instala: "
            "`pip install playwright && playwright install chromium`."
        )

    if not _openpyxl_disponible():
        st.info(
            "No se encontró `openpyxl`. Se usará un lector `.xlsx` alternativo; "
            "si encuentras limitaciones instala `openpyxl` con: `pip install openpyxl`."
        )

    st.session_state.setdefault("espera_captcha", 30)
    st.session_state["espera_captcha"] = st.slider(
        "Segundos de espera extra por URL",
        min_value=5,
        max_value=120,
        value=st.session_state["espera_captcha"],
    )

    entorno_con_pantalla = _entorno_tiene_pantalla()

    modo_manual_captcha = st.checkbox(
        "Pausar para resolver reCAPTCHA manualmente antes de guardar el PDF",
        value=entorno_con_pantalla,
        help=(
            "Desactiva el modo headless y deja abierta la ventana del navegador para que puedas "
            "hacer click en 'No soy un robot' cuando aparezca."
        ),
    )

    if modo_manual_captcha and not entorno_con_pantalla:
        st.warning(
            "Este entorno no tiene interfaz gráfica (DISPLAY/WAYLAND), por lo que el "
            "modo manual se ejecutará en headless automáticamente."
        )

    max_espera_resolucion_s = st.slider(
        "Tiempo máximo de espera para resolver reCAPTCHA (segundos)",
        min_value=30,
        max_value=300,
        value=120,
        step=10,
        disabled=not modo_manual_captcha,
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

    if st.button(
        "Procesar URLs y generar PDFs",
        type="primary",
        disabled=not playwright_instalado,
        help=(
            "Instala playwright para habilitar este botón."
            if not playwright_instalado
            else None
        ),
    ):
        progreso = st.empty()
        inicio = time.time()

        try:
            pdf_paths = guardar_paginas_como_pdf(
                urls=urls,
                progreso_placeholder=progreso,
                espera_captcha_s=st.session_state["espera_captcha"],
                modo_manual_captcha=modo_manual_captcha,
                max_espera_resolucion_s=max_espera_resolucion_s,
            )
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

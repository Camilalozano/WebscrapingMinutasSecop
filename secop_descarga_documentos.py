"""Descargador de documentos SECOP (enfoque en archivo fuente, no captura de pantalla).

Flujo:
1) Lee URLs desde Excel o CSV.
2) Abre cada URL con Playwright.
3) Permite resolución manual de captcha en modo visual (opcional).
4) Detecta enlaces/embeds de documentos.
5) Descarga el archivo binario real usando la sesión autenticada del navegador.
6) Genera manifest.csv con estados.

Uso rápido:
    python secop_descarga_documentos.py \
        --input URL_SECOP.xlsx \
        --output-dir output_docs \
        --headed --manual-captcha
"""

from __future__ import annotations

import argparse
import csv
import re
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse

import pandas as pd
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


ALLOWED_EXTENSIONS = {
    ".pdf",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".csv",
    ".zip",
    ".rar",
    ".7z",
    ".txt",
}

NON_DOCUMENT_CONTENT_TYPES = {
    "text/html",
    "application/xhtml+xml",
}


@dataclass
class DownloadResult:
    source_url: str
    status: str
    saved_files: str
    detail: str


def read_urls(input_path: Path) -> List[str]:
    """Lee URLs desde .xlsx/.xls/.csv y elimina duplicados preservando orden."""
    suffix = input_path.suffix.lower()

    if suffix in {".xlsx", ".xls"}:
        df = pd.read_excel(input_path)
    elif suffix == ".csv":
        df = pd.read_csv(input_path)
    else:
        raise ValueError("Formato no soportado. Usa .xlsx, .xls o .csv")

    values: Iterable[object] = [value for _, row in df.iterrows() for value in row.tolist()]
    urls: List[str] = []

    for value in values:
        if pd.isna(value):
            continue
        text = str(value).strip()
        if not text:
            continue

        match = re.search(r"https?://[^\s'\"}]+", text)
        if match:
            urls.append(match.group(0))

    return list(dict.fromkeys(urls))


def sanitize_filename(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]", "_", name).strip("._")
    return clean or f"document_{int(time.time())}"


def detect_captcha(page) -> bool:
    selectors = [
        "iframe[src*='recaptcha']",
        "iframe[title*='reCAPTCHA']",
        "textarea[name='g-recaptcha-response']",
        "text=No soy un robot",
        "text=No soy robot",
        "text=verifica que eres humano",
        "text=verify you are human",
    ]
    for selector in selectors:
        try:
            if page.locator(selector).count() > 0:
                return True
        except Exception:
            continue

    return False


def wait_manual_captcha_resolution(page, max_wait_s: int) -> bool:
    start = time.time()
    while time.time() - start < max_wait_s:
        if not detect_captcha(page):
            return True
        page.wait_for_timeout(1000)
    return False


def extract_candidate_document_urls(page, base_url: str) -> List[str]:
    candidates = page.evaluate(
        """
        () => {
          const out = new Set();
          const push = (v) => { if (v && typeof v === 'string') out.add(v.trim()); };

          document.querySelectorAll('a[href]').forEach(a => push(a.getAttribute('href')));
          document.querySelectorAll('iframe[src]').forEach(el => push(el.getAttribute('src')));
          document.querySelectorAll('embed[src]').forEach(el => push(el.getAttribute('src')));
          document.querySelectorAll('object[data]').forEach(el => push(el.getAttribute('data')));
          document.querySelectorAll('source[src]').forEach(el => push(el.getAttribute('src')));

          return Array.from(out);
        }
        """
    )

    normalized: List[str] = []
    for value in candidates:
        full = urljoin(base_url, value)
        normalized.append(full)

    filtered: List[str] = []
    for url in normalized:
        path = urlparse(url).path.lower()
        if any(path.endswith(ext) for ext in ALLOWED_EXTENSIONS):
            filtered.append(url)
            continue

        if any(token in url.lower() for token in ["download", "archivo", "documento", "adjunto"]):
            filtered.append(url)

    return list(dict.fromkeys(filtered))


def _filename_from_headers_or_url(response, url: str, fallback_prefix: str) -> str:
    cd = response.headers.get("content-disposition", "")
    match = re.search(r"filename\*=UTF-8''([^;]+)|filename=\"?([^\";]+)\"?", cd, re.I)
    if match:
        raw = match.group(1) or match.group(2)
        return sanitize_filename(raw)

    path_name = Path(urlparse(url).path).name
    if path_name:
        return sanitize_filename(path_name)

    content_type = (response.headers.get("content-type", "") or "").split(";")[0].strip().lower()
    ext_map = {
        "application/pdf": ".pdf",
        "application/zip": ".zip",
        "application/msword": ".doc",
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document": ".docx",
        "application/vnd.ms-excel": ".xls",
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": ".xlsx",
    }
    ext = ext_map.get(content_type, ".bin")
    return f"{fallback_prefix}{ext}"


def download_document(context, doc_url: str, target_dir: Path, fallback_prefix: str) -> Optional[Path]:
    response = context.request.get(doc_url, timeout=120_000)
    if not response.ok:
        return None

    content_type = (response.headers.get("content-type", "") or "").split(";")[0].strip().lower()
    if content_type in NON_DOCUMENT_CONTENT_TYPES:
        return None

    body = response.body()
    if len(body) < 1024:
        return None

    filename = _filename_from_headers_or_url(response, doc_url, fallback_prefix)
    filepath = target_dir / filename

    counter = 1
    while filepath.exists():
        filepath = target_dir / f"{filepath.stem}_{counter}{filepath.suffix}"
        counter += 1

    filepath.write_bytes(body)
    return filepath


def process_url(
    context,
    page,
    source_url: str,
    target_dir: Path,
    manual_captcha: bool,
    max_wait_captcha_s: int,
) -> DownloadResult:
    try:
        page.goto(source_url, wait_until="domcontentloaded", timeout=120_000)
        try:
            page.wait_for_load_state("networkidle", timeout=20_000)
        except PlaywrightTimeoutError:
            pass

        if detect_captcha(page):
            if not manual_captcha:
                return DownloadResult(source_url, "captcha_detected", "", "Captcha detectado; activa --manual-captcha")

            resolved = wait_manual_captcha_resolution(page, max_wait_captcha_s)
            if not resolved:
                return DownloadResult(source_url, "captcha_timeout", "", "No se resolvió captcha a tiempo")

        candidates = extract_candidate_document_urls(page, source_url)
        if not candidates:
            return DownloadResult(source_url, "no_candidates", "", "No se detectaron enlaces de documento")

        saved: List[Path] = []
        for i, doc_url in enumerate(candidates, start=1):
            saved_path = download_document(context, doc_url, target_dir, fallback_prefix=f"doc_{i}")
            if saved_path:
                saved.append(saved_path)

        if not saved:
            return DownloadResult(source_url, "download_failed", "", "No se pudo descargar ningún candidato")

        return DownloadResult(source_url, "ok", "|".join(str(p) for p in saved), f"{len(saved)} archivo(s)")

    except Exception as exc:
        return DownloadResult(source_url, "error", "", str(exc))


def write_manifest(results: List[DownloadResult], output_dir: Path) -> Path:
    manifest_path = output_dir / "manifest.csv"
    with manifest_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["source_url", "status", "saved_files", "detail"])
        writer.writeheader()
        for row in results:
            writer.writerow(asdict(row))
    return manifest_path


def run_pipeline(
    input_file: Path,
    output_dir: Path,
    headed: bool,
    manual_captcha: bool,
    max_wait_captcha_s: int,
) -> None:
    urls = read_urls(input_file)
    if not urls:
        raise RuntimeError("No se detectaron URLs en el archivo de entrada")

    output_dir.mkdir(parents=True, exist_ok=True)

    results: List[DownloadResult] = []
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=not headed)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        for idx, url in enumerate(urls, start=1):
            print(f"[{idx}/{len(urls)}] Procesando: {url}")
            result = process_url(
                context=context,
                page=page,
                source_url=url,
                target_dir=output_dir,
                manual_captcha=manual_captcha,
                max_wait_captcha_s=max_wait_captcha_s,
            )
            print(f"    -> {result.status}: {result.detail}")
            results.append(result)

        browser.close()

    manifest = write_manifest(results, output_dir)
    ok_count = sum(1 for r in results if r.status == "ok")
    print(f"\nCompletado. URLs con descarga exitosa: {ok_count}/{len(results)}")
    print(f"Manifest: {manifest}")


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Descargador de documentos SECOP")
    parser.add_argument("--input", required=True, type=Path, help="Archivo .xlsx/.xls/.csv con URLs")
    parser.add_argument("--output-dir", type=Path, default=Path("output_docs"), help="Carpeta de salida")
    parser.add_argument("--headed", action="store_true", help="Abre navegador visible")
    parser.add_argument(
        "--manual-captcha",
        action="store_true",
        help="Si hay captcha, espera resolución manual en navegador visible",
    )
    parser.add_argument(
        "--max-wait-captcha-s",
        type=int,
        default=180,
        help="Tiempo máximo para resolver captcha manualmente",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()

    if args.manual_captcha and not args.headed:
        raise ValueError("--manual-captcha requiere --headed para interacción humana")

    run_pipeline(
        input_file=args.input,
        output_dir=args.output_dir,
        headed=args.headed,
        manual_captcha=args.manual_captcha,
        max_wait_captcha_s=args.max_wait_captcha_s,
    )


if __name__ == "__main__":
    main()

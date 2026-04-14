"""Extracción robusta de variables de minutas SECOP usando IA (OpenAI).

Entradas soportadas:
- Carpeta con PDFs
- Archivo .zip con PDFs

Salida:
- Excel (.xlsx) con las variables:
  numero_contrato
  tipo_contrato
  nombre_contratista
  numero_documento_contratista
  obligaciones_especificas
  nombre_supervisor

Uso:
  python extract_variables_contratos_ai.py \
      --input Seleccion_500_Archivos_contratos_pdf.zip \
      --output contratos_extraidos.xlsx \
      --model gpt-4.1-mini

Requisitos:
- OPENAI_API_KEY configurada en variables de entorno.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import tempfile
import zipfile
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from openai import OpenAI
from pypdf import PdfReader


@dataclass
class ContractExtraction:
    archivo_origen: str
    numero_contrato: str
    tipo_contrato: str
    nombre_contratista: str
    numero_documento_contratista: str
    obligaciones_especificas: str
    nombre_supervisor: str
    confianza_modelo: str
    observaciones: str


SYSTEM_PROMPT = (
    "Eres un analista legal experto en contratación pública colombiana. "
    "Extrae variables de minutas/contratos SECOP. "
    "Responde SOLO JSON válido, sin markdown, sin comentarios."
)


EXTRACTION_SCHEMA = {
    "type": "object",
    "properties": {
        "numero_contrato": {"type": "string"},
        "tipo_contrato": {"type": "string"},
        "nombre_contratista": {"type": "string"},
        "numero_documento_contratista": {"type": "string"},
        "obligaciones_especificas": {"type": "string"},
        "nombre_supervisor": {"type": "string"},
        "confianza_modelo": {
            "type": "string",
            "enum": ["alta", "media", "baja"],
        },
        "observaciones": {"type": "string"},
    },
    "required": [
        "numero_contrato",
        "tipo_contrato",
        "nombre_contratista",
        "numero_documento_contratista",
        "obligaciones_especificas",
        "nombre_supervisor",
        "confianza_modelo",
        "observaciones",
    ],
    "additionalProperties": False,
}


def _normalizar_texto(text: str) -> str:
    text = text.replace("\x00", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _chunk_text(text: str, max_chars: int = 12000, overlap: int = 1200) -> List[str]:
    if len(text) <= max_chars:
        return [text]

    chunks: List[str] = []
    start = 0
    while start < len(text):
        end = min(len(text), start + max_chars)
        chunks.append(text[start:end])
        if end == len(text):
            break
        start = max(0, end - overlap)
    return chunks


def _extract_text_from_pdf(pdf_path: Path) -> str:
    reader = PdfReader(str(pdf_path))
    pages = [page.extract_text() or "" for page in reader.pages]
    text = "\n".join(pages)
    return _normalizar_texto(text)


def _regex_fallback(text: str) -> dict:
    patterns = {
        "numero_contrato": [
            r"(?:n[oúu]mero|no\.?|n°)\s*(?:del\s*)?contrato\s*[:\-]?\s*([A-Z0-9\-_/\.]+)",
            r"contrato\s*[:\-#]?\s*([A-Z0-9\-_/\.]{4,})",
        ],
        "tipo_contrato": [
            r"tipo\s+de\s+contrato\s*[:\-]?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ\s\-]{6,80})",
            r"modalidad\s*[:\-]?\s*([A-Za-zÁÉÍÓÚÑáéíóúñ\s\-]{6,80})",
        ],
        "nombre_contratista": [
            r"contratista\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\s\.,\-]{5,120})",
        ],
        "numero_documento_contratista": [
            r"(?:c[ée]dula|nit|documento\s+de\s+identidad)\s*(?:no\.?|n[oúu]mero)?\s*[:\-]?\s*([0-9\.\-]{5,20})",
        ],
        "nombre_supervisor": [
            r"supervisor(?:a)?\s*[:\-]?\s*([A-ZÁÉÍÓÚÑ][A-Za-zÁÉÍÓÚÑáéíóúñ\s\.,\-]{5,120})",
        ],
    }

    out = {
        "numero_contrato": "",
        "tipo_contrato": "",
        "nombre_contratista": "",
        "numero_documento_contratista": "",
        "obligaciones_especificas": "",
        "nombre_supervisor": "",
    }

    for key, regexes in patterns.items():
        for rx in regexes:
            m = re.search(rx, text, flags=re.IGNORECASE)
            if m:
                out[key] = m.group(1).strip(" .;,-")
                break

    obligations_match = re.search(
        r"obligaciones\s+espec[ií]ficas\s*[:\-]?\s*(.{120,3000}?)(?:obligaciones\s+generales|plazo|valor|supervisor|$)",
        text,
        flags=re.IGNORECASE,
    )
    if obligations_match:
        out["obligaciones_especificas"] = obligations_match.group(1).strip()

    return out


def _build_user_prompt(text_chunk: str, initial_guess: dict) -> str:
    return (
        "Extrae las variables del contrato y devuelve SOLO un JSON válido. "
        "Si no encuentras un campo, devuelve cadena vacía. "
        "Para 'obligaciones_especificas', resume de forma textual y clara los puntos clave. "
        "Para 'nombre_supervisor', busca supervisor(a), interventor(a) o quien ejerza supervisión. "
        "No inventes datos.\n\n"
        f"Pista inicial por regex (puede estar incompleta): {json.dumps(initial_guess, ensure_ascii=False)}\n\n"
        f"Texto:\n{text_chunk}"
    )


def _call_openai_extract(client: OpenAI, model: str, text_chunk: str, initial_guess: dict) -> dict:
    response = client.responses.create(
        model=model,
        input=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": _build_user_prompt(text_chunk, initial_guess)},
        ],
        text={
            "format": {
                "type": "json_schema",
                "name": "contract_extraction",
                "schema": EXTRACTION_SCHEMA,
                "strict": True,
            }
        },
    )

    return json.loads(response.output_text)


def _merge_extractions(extractions: Iterable[dict]) -> dict:
    merged = {
        "numero_contrato": "",
        "tipo_contrato": "",
        "nombre_contratista": "",
        "numero_documento_contratista": "",
        "obligaciones_especificas": "",
        "nombre_supervisor": "",
        "confianza_modelo": "baja",
        "observaciones": "",
    }

    confianza_rank = {"baja": 0, "media": 1, "alta": 2}
    seen_obligaciones: List[str] = []

    for item in extractions:
        for field in [
            "numero_contrato",
            "tipo_contrato",
            "nombre_contratista",
            "numero_documento_contratista",
            "nombre_supervisor",
        ]:
            if not merged[field] and item.get(field):
                merged[field] = item[field].strip()

        obligations = (item.get("obligaciones_especificas") or "").strip()
        if obligations and obligations not in seen_obligaciones:
            seen_obligaciones.append(obligations)

        current_rank = confianza_rank.get(merged["confianza_modelo"], 0)
        item_rank = confianza_rank.get(item.get("confianza_modelo", "baja"), 0)
        if item_rank > current_rank:
            merged["confianza_modelo"] = item.get("confianza_modelo", "baja")

        obs = (item.get("observaciones") or "").strip()
        if obs:
            merged["observaciones"] = f"{merged['observaciones']} | {obs}".strip(" |")

    merged["obligaciones_especificas"] = "\n\n".join(seen_obligaciones[:3])
    return merged


def process_pdf(client: OpenAI, model: str, pdf_path: Path) -> ContractExtraction:
    text = _extract_text_from_pdf(pdf_path)
    regex_guess = _regex_fallback(text)

    if not text:
        return ContractExtraction(
            archivo_origen=pdf_path.name,
            numero_contrato="",
            tipo_contrato="",
            nombre_contratista="",
            numero_documento_contratista="",
            obligaciones_especificas="",
            nombre_supervisor="",
            confianza_modelo="baja",
            observaciones="PDF sin texto legible (posible escaneo/OCR requerido)",
        )

    chunks = _chunk_text(text)
    extractions = []
    for chunk in chunks[:4]:
        try:
            data = _call_openai_extract(client, model, chunk, regex_guess)
            extractions.append(data)
        except Exception as exc:
            extractions.append(
                {
                    "numero_contrato": regex_guess["numero_contrato"],
                    "tipo_contrato": regex_guess["tipo_contrato"],
                    "nombre_contratista": regex_guess["nombre_contratista"],
                    "numero_documento_contratista": regex_guess[
                        "numero_documento_contratista"
                    ],
                    "obligaciones_especificas": regex_guess["obligaciones_especificas"],
                    "nombre_supervisor": regex_guess["nombre_supervisor"],
                    "confianza_modelo": "baja",
                    "observaciones": f"Fallback regex por error de IA: {exc}",
                }
            )

    merged = _merge_extractions(extractions)

    return ContractExtraction(
        archivo_origen=pdf_path.name,
        numero_contrato=merged["numero_contrato"],
        tipo_contrato=merged["tipo_contrato"],
        nombre_contratista=merged["nombre_contratista"],
        numero_documento_contratista=merged["numero_documento_contratista"],
        obligaciones_especificas=merged["obligaciones_especificas"],
        nombre_supervisor=merged["nombre_supervisor"],
        confianza_modelo=merged["confianza_modelo"],
        observaciones=merged["observaciones"],
    )


def _collect_pdfs(input_path: Path, temp_dir: Optional[Path] = None) -> List[Path]:
    if input_path.is_dir():
        return sorted(input_path.glob("*.pdf"))

    if input_path.suffix.lower() == ".zip":
        if temp_dir is None:
            raise ValueError("temp_dir es obligatorio para procesar ZIP")
        with zipfile.ZipFile(input_path, "r") as zf:
            zf.extractall(temp_dir)
        return sorted(temp_dir.rglob("*.pdf"))

    if input_path.suffix.lower() == ".pdf":
        return [input_path]

    raise ValueError("Entrada no soportada. Usa carpeta, .zip o .pdf")


def run_extraction(input_path: Path, output_excel: Path, model: str) -> Path:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("No se encontró OPENAI_API_KEY en variables de entorno")

    client = OpenAI(api_key=api_key)

    with tempfile.TemporaryDirectory(prefix="secop_extract_") as td:
        temp_dir = Path(td)
        pdf_paths = _collect_pdfs(input_path, temp_dir=temp_dir)

        if not pdf_paths:
            raise RuntimeError("No se encontraron PDFs en la entrada")

        rows: List[ContractExtraction] = []
        for idx, pdf_path in enumerate(pdf_paths, start=1):
            print(f"[{idx}/{len(pdf_paths)}] Procesando: {pdf_path.name}")
            rows.append(process_pdf(client=client, model=model, pdf_path=pdf_path))

        df = pd.DataFrame([asdict(row) for row in rows])

        output_excel.parent.mkdir(parents=True, exist_ok=True)
        df.to_excel(output_excel, index=False)

    return output_excel


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Extraer variables de contratos SECOP con IA")
    parser.add_argument("--input", required=True, type=Path, help="Carpeta, .zip o .pdf")
    parser.add_argument("--output", required=True, type=Path, help="Ruta Excel de salida (.xlsx)")
    parser.add_argument("--model", default="gpt-4.1-mini", help="Modelo OpenAI para extracción")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    output_path = run_extraction(input_path=args.input, output_excel=args.output, model=args.model)
    print(f"Listo. Excel generado en: {output_path}")


if __name__ == "__main__":
    main()

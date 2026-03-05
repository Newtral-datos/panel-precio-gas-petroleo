"""
Actualización diaria automática de precios energéticos.
- Gas: descarga CSV de MIBGAS y actualiza gas_total.csv
- Petróleo: scraping de investing.com y actualiza petroleo.csv
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

BASE_DIR = Path(__file__).parent
GAS_DIR = BASE_DIR / "Gas"
OIL_DIR = BASE_DIR / "Petróleo"


def update_gas() -> bool:
    year = datetime.now().year
    url = (
        f"https://www.mibgas.es/es/file-access/MIBGAS_Data_{year}.csv"
        f"?path=AGNO_{year}/XLS"
    )
    local_csv = GAS_DIR / f"MIBGAS_Data_{year}.csv"
    gas_total = GAS_DIR / "gas_total.csv"

    # --- Descarga ---
    log.info("Descargando %s", url)
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exc:
        log.warning("Error al descargar datos de gas: %s", exc)
        return False

    local_csv.write_bytes(resp.content)
    log.info("Guardado %s", local_csv)

    # --- Parseo ---
    try:
        df_raw = pd.read_csv(local_csv, sep=";", skiprows=1)
        df_raw.columns = [c.strip().strip('"') for c in df_raw.columns]

        df = df_raw[df_raw["Product"] == "GDAES_D+1"].copy()
        df = df.dropna(subset=["MIBGAS Daily Price [EUR/MWh]"])
        df["Fecha"] = pd.to_datetime(df["Last Day Delivery"], format="%d/%m/%Y", errors="coerce")
        df["Precio_EUR_MWh"] = pd.to_numeric(
            df["MIBGAS Daily Price [EUR/MWh]"], errors="coerce"
        )
        df["Año"] = year
        df = df[df["Fecha"].dt.year == year]  # solo fechas del año actual
        df["Fecha"] = df["Fecha"].dt.strftime("%Y-%m-%d")
        df_new = df[["Fecha", "Precio_EUR_MWh", "Año"]].copy()
    except Exception as exc:
        log.warning("Error al parsear datos de gas: %s", exc)
        return False

    if df_new.empty:
        log.info("No hay datos nuevos de gas (posible festivo / fin de semana)")
        return True

    log.info("Filas nuevas de gas: %d", len(df_new))

    # --- Actualiza gas_total.csv ---
    try:
        df_total = pd.read_csv(gas_total)
        df_total = df_total[df_total["Año"] != year]
        df_total = pd.concat([df_total, df_new], ignore_index=True)
        df_total = df_total.sort_values("Fecha").reset_index(drop=True)
        df_total.to_csv(gas_total, index=False)
        log.info("gas_total.csv actualizado (%d filas)", len(df_total))
    except Exception as exc:
        log.warning("Error al actualizar gas_total.csv: %s", exc)
        return False

    return True


def _parse_spanish_float(text: str) -> float:
    """Convierte '1.234,56' o '81,40' a float."""
    text = text.strip()
    # Si hay punto de miles y coma decimal: quitar punto, cambiar coma por punto
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    return float(text)


def update_oil() -> bool:
    oil_csv = OIL_DIR / "petroleo.csv"

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PWTimeout
    except ImportError:
        log.warning("Playwright no instalado; omitiendo actualización de petróleo")
        return False

    log.info("Iniciando scraping de investing.com (Brent)")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/124.0.0.0 Safari/537.36"
            ),
            locale="es-ES",
        )
        page = context.new_page()

        try:
            page.goto(
                "https://es.investing.com/commodities/brent-oil-historical-data",
                wait_until="domcontentloaded",
                timeout=60_000,
            )
        except Exception as exc:
            log.warning("Error al navegar a investing.com: %s", exc)
            browser.close()
            return False

        # Acepta cookies si aparece el banner
        for selector in [
            "#onetrust-accept-btn-handler",
            "button[id*='accept']",
            "button[class*='accept']",
        ]:
            try:
                btn = page.locator(selector).first
                if btn.is_visible(timeout=3_000):
                    btn.click()
                    log.info("Banner de cookies aceptado")
                    break
            except Exception:
                pass

        # Espera la tabla de datos históricos
        row = None
        for selector in [
            "tr[class*='historical-data-v2_price']",
            "table tbody tr",
        ]:
            try:
                page.wait_for_selector(selector, timeout=20_000)
                row = page.locator(selector).nth(1)  # segunda fila = día anterior (precio definitivo)
                log.info("Tabla encontrada con selector: %s", selector)
                break
            except PWTimeout:
                log.info("Selector '%s' no encontrado, probando fallback", selector)

        if row is None:
            log.warning("No se encontró ninguna fila de datos históricos")
            browser.close()
            return False

        try:
            time_el = row.locator("time").first
            date_str = time_el.get_attribute("datetime") or time_el.inner_text()
            date_str = date_str.strip()
            # Formatos posibles: DD.MM.YYYY (es.investing.com), YYYY-MM-DD, DD/MM/YYYY
            if "." in date_str and len(date_str) == 10:
                fecha = datetime.strptime(date_str, "%d.%m.%Y").strftime("%Y-%m-%d")
            elif "/" in date_str:
                fecha = datetime.strptime(date_str, "%d/%m/%Y").strftime("%Y-%m-%d")
            else:
                fecha = pd.to_datetime(date_str).strftime("%Y-%m-%d")

            tds = row.locator("td")
            price_text = tds.nth(1).inner_text()
            price = _parse_spanish_float(price_text)
        except Exception as exc:
            log.warning("Error al extraer precio de Brent: %s", exc)
            browser.close()
            return False

        browser.close()

    log.info("Brent: fecha=%s precio=%.2f", fecha, price)

    # --- Actualiza petroleo.csv ---
    try:
        df = pd.read_csv(oil_csv, sep=";")
        existing_dates = set(df["fecha"].astype(str))
        if fecha in existing_dates:
            log.info("Fecha %s ya existe en petroleo.csv; sin cambios", fecha)
            return True

        new_row = pd.DataFrame([{"fecha": fecha, "precio": price}])
        df = pd.concat([df, new_row], ignore_index=True)
        df = df.sort_values("fecha").reset_index(drop=True)
        df.to_csv(oil_csv, index=False, sep=";")
        log.info("petroleo.csv actualizado (%d filas)", len(df))
    except Exception as exc:
        log.warning("Error al actualizar petroleo.csv: %s", exc)
        return False

    return True


def main():
    log.info("=== Inicio actualización de datos energéticos ===")

    gas_ok = update_gas()
    log.info("Gas: %s", "OK" if gas_ok else "ADVERTENCIA (sin cambios o error)")

    oil_ok = update_oil()
    log.info("Petróleo: %s", "OK" if oil_ok else "ADVERTENCIA (sin cambios o error)")

    log.info("=== Fin ===")
    sys.exit(0)  # Siempre código 0 para no romper el workflow


if __name__ == "__main__":
    main()

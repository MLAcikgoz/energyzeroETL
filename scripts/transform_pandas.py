import pandas as pd
import json
import glob
import os

def transform_latest_json():
    os.makedirs("data/processed", exist_ok=True)

    # En son indirilen JSON dosyasını bul
    latest_file = sorted(glob.glob("data/raw/*.json"))[-1]
    print(f"🔍 İşlenecek dosya: {latest_file}")

    # JSON dosyasını oku
    with open(latest_file, "r") as f:
        data = json.load(f)

    # "Prices" (büyük P) veya "prices" alanını kontrol et
    if "Prices" in data:
        df = pd.DataFrame(data["Prices"])
    elif "prices" in data:
        df = pd.DataFrame(data["prices"])
    else:
        # Eğer ikisi de yoksa, tüm veriden DataFrame oluştur
        df = pd.DataFrame(data)

    # Kolon isimlerini kontrol et
    print("📊 Kolonlar:", df.columns.tolist())

    # readingDate sütunu varsa tarih/saat işlemleri yap
    if "readingDate" in df.columns:
        df["Date"] = pd.to_datetime(df["readingDate"]).dt.date
        df["Time"] = pd.to_datetime(df["readingDate"]).dt.time
    else:
        raise KeyError("Veride 'readingDate' alanı bulunamadı.")

    # price sütunu varsa %21 vergi ekle
    if "price" in df.columns:
        df["Price_with_VAT"] = df["price"] * 1.21
    else:
        raise KeyError("Veride 'price' alanı bulunamadı.")

    # Parquet formatında kaydet
    output_file = "data/processed/energy_transformed.parquet"
    df.to_parquet(output_file, index=False)
    print(f"✅ Saved processed data: {output_file}")

if __name__ == "__main__":
    transform_latest_json()


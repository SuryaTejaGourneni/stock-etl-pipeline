from src.extract import extract_all
from src.transform import transform
from src.load import load

def run_pipeline():
    print("=" * 40)
    print("STOCK ETL PIPELINE STARTING")
    print("=" * 40)

    print("\n[1/3] EXTRACTING...")
    df_raw = extract_all()
    if df_raw is None:
        print("Pipeline failed at extraction.")
        return

    print("\n[2/3] TRANSFORMING...")
    df_clean = transform(df_raw)

    print("\n[3/3] LOADING...")
    load(df_clean)

    print("\n✅ Pipeline complete!")

if __name__ == "__main__":
    run_pipeline()


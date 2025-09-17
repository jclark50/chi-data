import os, sys

def main():
    path = os.environ.get("ERA5_INPUT", "data/era5/hourly")
    print(f"[extract] Using ERA5 input at: {path}")
    # In a real pipeline, you might validate partitions here.
    return 0

if __name__ == "__main__":
    sys.exit(main())
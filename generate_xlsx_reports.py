"""Load json dataset, create reports objects, save excel to scripts/reports/data_out/."""
import json

from pathlib import Path

import prism.reports
import prism.types


brand = "orangetheoryfitness"
output_file = "output.xlsx"

DIR_BACKEND = Path(__file__).resolve().parent.parent.parent
DIR_SAVE = DIR_BACKEND / "scripts" / "reports" / "data_out"
dataset_file = DIR_BACKEND / "dataset" / "brands" / (brand + ".json")


def main():
    """Create xlsx roll up for all stores in dataset."""
    with open(dataset_file, "r") as f:
        dataset = json.load(f)

    store_batch = prism.types.StoreBatch.from_dict(store_data=dataset)
    report_batch = prism.reports.generate_reports(store_batch)

    if not report_batch.reports:
        print("No reports were created.")
        return

    f_output = prism.reports.reports2xlsx(
        report_batch=report_batch,
        save_file=DIR_SAVE / output_file,
    )

    if f_output:
        print(
            f"Excel file '{DIR_SAVE / output_file}' with multiple sheets has been created."
        )
    else:
        print("No Excel file was created.  Check logs.")


if __name__ == "__main__":
    main()

from pathlib import Path

import polars as pl
import polars.selectors as cs

BASE_PATH = Path(__file__).resolve().parents[1]


def main():
    df = (
        pl.scan_csv(BASE_PATH / "data/raw/acs5_sex_age_by_zcta5.csv", null_values="NA")
        .drop("moe")
        .rename({"NAME": "zcta5"})
        .with_columns(pl.col("zcta5").str.replace_all("ZCTA5 ", "").cast(pl.Int64))
        .collect()
        .pivot(values="estimate", index=["zcta5", "year"], columns="variable")
        .select(cs.all().name.map(lambda c: c.replace("B01001_", "")))
        .rename(
            {
                "001": "total",
                "002": "male",
                "003": "male_l_5",
                "004": "male_5_9",
                "005": "male_10_14",
                "006": "male_15_17",
                "007": "male_18_19",
                "008": "male_20",
                "009": "male_21",
                "010": "male_22_24",
                "011": "male_25_29",
                "012": "male_30_34",
                "013": "male_35_39",
                "014": "male_40_44",
                "015": "male_45_49",
                "016": "male_50_54",
                "017": "male_55_59",
                "018": "male_60_61",
                "019": "male_62_64",
                "020": "male_65_66",
                "021": "male_67_69",
                "022": "male_70_74",
                "023": "male_75_79",
                "024": "male_80_84",
                "025": "male_ge_85",
                "026": "female",
                "027": "female_l_5",
                "028": "female_5_9",
                "029": "female_10_14",
                "030": "female_15_17",
                "031": "female_18_19",
                "032": "female_20",
                "033": "female_21",
                "034": "female_22_24",
                "035": "female_25_29",
                "036": "female_30_34",
                "037": "female_35_39",
                "038": "female_40_44",
                "039": "female_45_49",
                "040": "female_50_54",
                "041": "female_55_59",
                "042": "female_60_61",
                "043": "female_62_64",
                "044": "female_65_66",
                "045": "female_67_69",
                "046": "female_70_74",
                "047": "female_75_79",
                "048": "female_80_84",
                "049": "female_ge_85",
            }
        )
        .to_pandas()
    )

    df["l_62"] = df[
        [
            "male_l_5",
            "male_5_9",
            "male_10_14",
            "male_15_17",
            "male_18_19",
            "male_20",
            "male_21",
            "male_22_24",
            "male_25_29",
            "male_30_34",
            "male_35_39",
            "male_40_44",
            "male_45_49",
            "male_50_54",
            "male_55_59",
            "male_60_61",
            "female_l_5",
            "female_5_9",
            "female_10_14",
            "female_15_17",
            "female_18_19",
            "female_20",
            "female_21",
            "female_22_24",
            "female_25_29",
            "female_30_34",
            "female_35_39",
            "female_40_44",
            "female_45_49",
            "female_50_54",
            "female_55_59",
            "female_60_61",
        ]
    ].sum(axis=1)

    df["ge_62"] = df[
        [
            "male_62_64",
            "male_65_66",
            "male_67_69",
            "male_70_74",
            "male_75_79",
            "male_80_84",
            "male_ge_85",
            "female_62_64",
            "female_65_66",
            "female_67_69",
            "female_70_74",
            "female_75_79",
            "female_80_84",
            "female_ge_85",
        ]
    ].sum(axis=1)

    df["18_62"] = df["l_62"] - df[
        [
            "male_l_5",
            "male_5_9",
            "male_10_14",
            "male_15_17",
            "female_l_5",
            "female_5_9",
            "female_10_14",
            "female_15_17",
        ]
    ].sum(axis=1)

    sex = df[["zcta5", "year", "male", "female"]]
    age = df[["zcta5", "year", "l_62", "18_62", "ge_62"]]

    sex.to_parquet(BASE_PATH / "data/sex_by_zcta5.parquet", index=False)
    age.to_parquet(BASE_PATH / "data/age_by_zcta5.parquet", index=False)


    df = (
        pl.scan_csv(BASE_PATH / "data/raw/acs5_race_by_zcta5.csv", null_values="NA")
        .drop("GEOID", "moe")
        .rename({"NAME": "zcta5"})
        .with_columns(pl.col("zcta5").str.replace_all("ZCTA5 ", "").cast(pl.Int64))
        .collect()
        .pivot(values="estimate", index=["zcta5", "year"], columns="variable")
        .select(cs.all().name.map(lambda c: c.replace("B03002_", "")))
        .rename(
            {
                "001": "total",
                "003": "nh_white",
                "004": "nh_black",
                "005": "nh_aian",
                "006": "nh_asian",
                "007": "nh_hpi",
                "008": "nh_other",
                "009": "nh_multiracial",
                "012": "hispanic",
            }
        )
        .to_pandas()
    )

    # Cast to pandas because we want their .add() behavior, which is to treat nulls as
    # 0 when there is another non-null value but to return null when all are null.
    # Polars does not have this option.

    df["nh_api"] = df["nh_asian"] + df["nh_hpi"]
    df = df[
        [
            "zcta5",
            "year",
            "total",
            "hispanic",
            "nh_white",
            "nh_black",
            "nh_aian",
            "nh_asian",
            "nh_api",
            "nh_hpi",
            "nh_other",
            "nh_multiracial",
        ]
    ]

    df.to_parquet(BASE_PATH / "data/race_by_zcta5.parquet", index=False)


if __name__ == "__main__":
    main()

import sys
sys.path.insert(0, '../..')
from utilities import utils
from pyspark.sql import Window
from pyspark.sql.functions import col, row_number

class CarCrashAnalysis:
    def __init__(self, spark, path_to_config_file):
        input_file_paths = utils.read_yaml(path_to_config_file).get("INPUT_FILENAME")
        self.df_charges = utils.load_csv_data_to_df(spark, input_file_paths.get("Charges"))
        self.df_damages = utils.load_csv_data_to_df(spark, input_file_paths.get("Damages"))
        self.df_endorse = utils.load_csv_data_to_df(spark, input_file_paths.get("Endorse"))
        self.df_primary_person = utils.load_csv_data_to_df(spark, input_file_paths.get("Primary_Person"))
        self.df_units = utils.load_csv_data_to_df(spark, input_file_paths.get("Units"))
        self.df_restrict = utils.load_csv_data_to_df(spark, input_file_paths.get("Restrict"))

    def male_accident_count(self, output_path, output_format):
        """
        The function calculates no of the male person killed in accidents . Filter on PRSN_GNDR_ID and PRSN_INJURY_SEV_ID
        param -> output_path - where output is stored , output_format  -  output format
        return -> The count of male have been killed in the crash.
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "MALE").filter(self.df_primary_person.PRSN_INJRY_SEV_ID=='KILLED')
        utils.write_output(df, output_path, output_format)
        return df.count()

    def two_wheeler_booked_count(self, output_path, output_format):
        """
        To find the count of two wheeler which are booked for crashes.
        Filtering on VEH_BODY_STYL_ID where the MOTORCYCLE is contained as MOTORCYCLE AND "POLICE MOTORCYCLE" Needs to be included.
        param -> output_path - where output is stored , output_format  -  output format
        return -> The count of booked two wheelers
        """
        df = self.df_units.filter(col("VEH_BODY_STYL_ID").contains("MOTORCYCLE"))
        utils.write_output(df, output_path, output_format)

        return df.count()

    def get_state_with_highest_female_accident(self, output_path, output_format):
        """
        To find the state with highest female accidents count.
        First need to filter the column "PRSN_GNDR_ID" to get the FEMALE containing rows. Then groupby on "DRVR_LIC_STATE_ID" to get the
        count with state with highest female accidents.
        param -> output_path - where output is stored , output_format  -  output format
        return -> The top state with highest female involved in accident.
        """
        df = self.df_primary_person.filter(self.df_primary_person.PRSN_GNDR_ID == "FEMALE"). \
            groupby("DRVR_LIC_STATE_ID").count(). \
            orderBy(col("count").desc())
        utils.write_output(df, output_path, output_format)

        return df.first().DRVR_LIC_STATE_ID

    def get_top_5to15_vehicle_make_contributing_to_injuries(self, output_path, output_format):
        """
        To find the top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death.
        Firstly filtering out the non null values of "VEH_MAKE_ID" . Then making a column TOT_CASUALTIES_CNT which contains the count of
        "UNKN_INJRY_CNT" column and "TOT_INJRY_CNT" as these contains the count of injury involved in the accident. Then grouping on
        "VEH_MAKE_ID" column to get the count of injuries of "TOT_CASUALTIES_CNT".
        param -> output_path - where output is stored , output_format  -  output format
        return -> The clist of top 5 to 15th vehicle make id
        """
        df = self.df_units.filter(self.df_units.VEH_MAKE_ID != "NA"). \
            withColumn('TOT_CASUALTIES_CNT', self.df_units[35] + self.df_units[36]). \
            groupby("VEH_MAKE_ID").sum("TOT_CASUALTIES_CNT"). \
            withColumnRenamed("sum(TOT_CASUALTIES_CNT)", "TOT_CASUALTIES_CNT_AGG"). \
            orderBy(col("TOT_CASUALTIES_CNT_AGG").desc())

        df_top_5_to_15 = df.limit(15).subtract(df.limit(5))
        utils.write_output(df_top_5_to_15, output_path, output_format)

        return [veh[0] for veh in df_top_5_to_15.select("VEH_MAKE_ID").collect()]

    def get_top_ethnic_users_crash_for_each_body_style(self, output_path, output_format):
        """
        To find top ethnic users which are involved in the crashes.
        Fisrtly filtering out "VEH_BODY_STYL_ID" and "PRSN_ETHNICITY_ID"  columns with non-null values, unknown values.
        Then grouping on "VEH_BODY_STYL_ID" "PRSN_ETHNICITY_ID" and using row_nuber concept to get the top ethnic user involved in the crash.
        Finds and show top ethnic user group of each unique body style that was involved in crashes

        param -> output_path - where output is stored , output_format  -  output format
        return -> NOne
        """
        w = Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc())
        df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            filter(~self.df_units.VEH_BODY_STYL_ID.isin(["NA", "UNKNOWN", "NOT REPORTED",
                                                         "OTHER  (EXPLAIN IN NARRATIVE)"])). \
            filter(~self.df_primary_person.PRSN_ETHNICITY_ID.isin(["NA", "UNKNOWN"])). \
            groupby("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID").count(). \
            withColumn("row", row_number().over(w)).filter(col("row") == 1).drop("row", "count")

        utils.write_output(df, output_path, output_format)

        df.show(truncate=False)

    def get_top_5zip_codes_with_alcohols_as_factor_for_crash(self, output_path, output_format):
        """

        To find top 5 Zip Codes with the highest number crashes with alcohols as the contributing factor to a crash
        Firstly filetring out "DRVR_ZIP" with non null values , also filtering "CONTRIB_FACTR_1_ID and CONTRIB_FACTR_2_ID" containing
        alcohol keyword. Then grouping by DRVR_ZIP to get the count.
        param -> output_path - where output is stored , output_format  -  output format
        return -> The list of zipcode
        """

        df = self.df_units.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            filter(self.df_primary_person.DRVR_ZIP != "NA"). \
            filter(col("CONTRIB_FACTR_1_ID").contains("ALCOHOL") | col("CONTRIB_FACTR_2_ID").contains("ALCOHOL")). \
            groupby("DRVR_ZIP").count().orderBy(col("count").desc()).limit(5)
        utils.write_output(df, output_path, output_format)

        return [r[0] for r in df.collect()]

    def get_crash_ids_with_no_damage(self, output_path, output_format):
        """
        To get counts of distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4.
        Firstly filtering out VEH_DMAG_SCL_1_ID with non values and also damage >4, filtering DAMAGED_PROPERTY on "NONE" which indicates
        no damage was sustained. Also filtering on "FIN_RESP_TYPE_ID" which shows that it has insurance.
        and car avails Insurance.
        param -> output_path - where output is stored , output_format  -  output format
        return -> The list of crashids
        """
        df = self.df_damages.join(self.df_units, on=["CRASH_ID"], how='inner'). \
            filter(
            (
                    (self.df_units.VEH_DMAG_SCL_1_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_1_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            ) | (
                    (self.df_units.VEH_DMAG_SCL_2_ID > "DAMAGED 4") &
                    (~self.df_units.VEH_DMAG_SCL_2_ID.isin(["NA", "NO DAMAGE", "INVALID VALUE"]))
            )
        ). \
            filter(self.df_damages.DAMAGED_PROPERTY == "NONE"). \
            filter(self.df_units.FIN_RESP_TYPE_ID == "PROOF OF LIABILITY INSURANCE")
        utils.write_output(df, output_path, output_format)

        return [r[0] for r in df.collect()]

    def get_top_5_vehicle_brand_contributing_to_accidents(self, output_path, output_format):
        """
        To find  the top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
        Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
        offences.
        Firstly find lists of top25 state list and top 10 used color .
        Filter DRVR_LIC_TYPE_ID column which consists of license, "CHARGE" which contains the word "speed".
        param -> output_path - where output is stored , output_format  -  output format
        return -> The list of top 5 vehicle brands.
        """
        top_25_state_list = [row[0] for row in self.df_units.filter(col("VEH_LIC_STATE_ID").cast("int").isNull()).
            groupby("VEH_LIC_STATE_ID").count().orderBy(col("count").desc()).limit(25).collect()]
        top_10_used_vehicle_colors = [row[0] for row in self.df_units.filter(self.df_units.VEH_COLOR_ID != "NA").
            groupby("VEH_COLOR_ID").count().orderBy(col("count").desc()).limit(10).collect()]

        df = self.df_charges.join(self.df_primary_person, on=['CRASH_ID'], how='inner'). \
            join(self.df_units, on=['CRASH_ID'], how='inner'). \
            filter(self.df_charges.CHARGE.contains("SPEED")). \
            filter(self.df_primary_person.DRVR_LIC_TYPE_ID.isin(["DRIVER LICENSE", "COMMERCIAL DRIVER LIC."])). \
            filter(self.df_units.VEH_COLOR_ID.isin(top_10_used_vehicle_colors)). \
            filter(self.df_units.VEH_LIC_STATE_ID.isin(top_25_state_list)). \
            groupby("VEH_MAKE_ID").count(). \
            orderBy(col("count").desc()).limit(5)

        utils.write_output(df, output_path, output_format)

        return [r[0] for r in df.collect()]
from pyspark.sql import SparkSession

import os
import sys
from services import vehicleAccident
from utilities import utils

if os.path.exists('src.zip'):
    sys.path.insert(0, 'src.zip')
else:
    sys.path.insert(0, './Code/src')

if __name__ == '__main__':
    # Initialize sparks session
    spark = SparkSession \
        .builder \
        .appName("CarCrashAnalysis") \
        .getOrCreate()

    config_file_path = "config.yaml"
    spark.sparkContext.setLogLevel("ERROR")

    cca = vehicleAccident.CarCrashAnalysis(spark, config_file_path)
    output_file_paths = utils.read_yaml(config_file_path).get("OUTPUT_PATH")
    file_format = utils.read_yaml(config_file_path).get("FILE_FORMAT")

    # 1. Find the number of crashes (accidents) in which number of persons killed are male?
    print("1. Result:", cca.male_accident_count(output_file_paths.get(1), file_format.get("Output")))

    # 2. How many two-wheelers are booked for crashes?
    print("2. Result:", cca.two_wheeler_booked_count(output_file_paths.get(2), file_format.get("Output")))

    # 3. Which state has the highest number of accidents in which females are involved?
    print("3. Result:", cca.get_state_with_highest_female_accident(output_file_paths.get(3),
                                                                     file_format.get("Output")))

    # 4. Which are the Top 5th to 15th VEH_MAKE_IDs that contribute to a largest number of injuries including death
    print("4. Result:", cca.get_top_5to15_vehicle_make_contributing_to_injuries(output_file_paths.get(4),
                                                                       file_format.get("Output")))

    # 5. For all the body styles involved in crashes, mention the top ethnic user group of each unique body style
    print("5. Result:")
    cca.get_top_ethnic_users_crash_for_each_body_style(output_file_paths.get(5), file_format.get("Output"))

    # 6. Among the crashed cars, what are the Top 5 Zip Codes with the highest number crashes with alcohols as the
    # contributing factor to a crash (Use Driver Zip Code)
    print("6. Result:", cca.get_top_5zip_codes_with_alcohols_as_factor_for_crash(output_file_paths.get(6),
                                                                                file_format.get("Output")))

    # 7. Count of Distinct Crash IDs where No Damaged Property was observed and Damage Level (VEH_DMAG_SCL~) is above 4
    # and car avails Insurance
    print("7. Result:", cca.get_crash_ids_with_no_damage(output_file_paths.get(7), file_format.get("Output")))

    # 8. Determine the Top 5 Vehicle Makes/Brands where drivers are charged with speeding related offences, has licensed
    # Drivers, uses top 10 used vehicle colours and has car licensed with the Top 25 states with highest number of
    # offences (to be deduced from the data)
    print("8. Result:", cca.get_top_5_vehicle_brand_contributing_to_accidents(output_file_paths.get(8), file_format.get("Output")))

    spark.stop()

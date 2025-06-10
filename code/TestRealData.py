#
# Tester for the assignement1
#
DATABASE_NAME = 'dds_assgn1'

# TODO: Change these as per your code
RATINGS_TABLE = 'ratings'
RANGE_TABLE_PREFIX = 'range_part'
RROBIN_TABLE_PREFIX = 'rrobin_part'
USER_ID_COLNAME = 'userid'
MOVIE_ID_COLNAME = 'movieid'
RATING_COLNAME = 'rating'
# INPUT_FILE_PATH = 'test_data.dat'
INPUT_FILE_PATH = 'data/ml-10M100K/ratings.dat'
# ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file
ACTUAL_ROWS_IN_INPUT_FILE = 10000054

import psycopg2
import traceback
import testHelper
import Interface as MyAssignment
import time

if __name__ == '__main__':
    try:
        testHelper.createdb(DATABASE_NAME)

        with testHelper.getopenconnection(dbname=DATABASE_NAME) as conn:
            conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

            testHelper.deleteAllPublicTables(conn)

            loadrating_startedTime = time.time()

            [result, e] = testHelper.testloadratings(MyAssignment, RATINGS_TABLE, INPUT_FILE_PATH, conn,
                                                     ACTUAL_ROWS_IN_INPUT_FILE)
            if result:
                print("loadratings function pass!")
            else:
                print("loadratings function fail!")

            loadrating_endedTime = time.time()
            loadrating_elapsedTime = loadrating_endedTime - loadrating_startedTime
            print(f"\nTổng thời gian load xong: {loadrating_elapsedTime:.3f} giây")

            # start_time = time.time()

            partition_choice = input("\nChọn thuật toán phân mảnh để test (range / roundrobin): ").strip().lower()

            if partition_choice == 'range':
                start_time = time.time()
                print("\nTesting RANGE partitioning...")

                range_partitioned_startedTime = time.time()

                [result, e] = testHelper.testrangepartition(MyAssignment, RATINGS_TABLE, 5, conn, 0,
                                                            ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print("rangepartition function pass!")
                else:
                    print("rangepartition function fail!")

                range_partitioned_endedTime = time.time()
                range_partitioned_elapsedTime = range_partitioned_endedTime - range_partitioned_startedTime
                print(f"\nTổng thời gian rangepartition: {range_partitioned_elapsedTime:.3f} giây")

                # # Tạm thời dừng
                # input("Nhập Enter để tiếp tục sau khi đã kiểm tra rangepartition...")

                range_inserted_startedTime = time.time()
                # Test rangeinsert function
                [result, e] = testHelper.testrangeinsert(MyAssignment, RATINGS_TABLE, 100, 2, 3, conn, '2')
                if result:
                    print("rangeinsert function pass!")
                else:
                    print("rangeinsert function fail!")
                range_inserted_endedTime = time.time()
                range_inserted_elapsedTime = range_inserted_endedTime - range_inserted_startedTime
                print(f"\nTổng thời gian rangeinsert: {range_inserted_elapsedTime:.3f} giây")

            elif partition_choice == 'roundrobin':

                print("\nTesting ROUND ROBIN partitioning...")
                round_robin_partition_startedTime = time.time()
                [result, e] = testHelper.testroundrobinpartition(MyAssignment, RATINGS_TABLE, 5, conn, 0,
                                                                 ACTUAL_ROWS_IN_INPUT_FILE)
                if result:
                    print("roundrobinpartition function pass!")
                else:
                    print("roundrobinpartition function fail!")

                round_robin_partition_endedTime = time.time()
                round_robin_elapsedTime = round_robin_partition_endedTime - round_robin_partition_startedTime
                print(f"\nTổng thời gian roundrobinpartition: {round_robin_elapsedTime:.3f} giây")

                # # Tạm thời dừng
                # input("Nhập Enter để tiếp tục sau khi đã kiểm tra roundrobinpartition...")

                round_robin_inserted_startedTime = time.time()
                # Test roundrobininsert function
                # [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '0')
                [result, e] = testHelper.testroundrobininsert(MyAssignment, RATINGS_TABLE, 100, 1, 3, conn, '4')
                if result:
                    print("roundrobininsert function pass!")
                else:
                    print("roundrobininsert function fail!")
                round_robin_inserted_endedTime = time.time()
                round_robin_inserted_elapsedTime = round_robin_inserted_endedTime - round_robin_inserted_startedTime
                print(f"\nTổng thời gian roundrobininsert: {round_robin_inserted_elapsedTime:.3f} giây")
            else:
                print("Lựa chọn không hợp lệ! Vui lòng chạy lại và chọn 'range' hoặc 'roundrobin'.")

            # Kết thúc tính thời gian
            # end_time = time.time()
            # elapsed_time = end_time - start_time
            # print(f"\nTổng thời gian từ khi load xong đến khi phân mảnh + insert hoàn tất: {elapsed_time:.3f} giây")

            choice = input('Press enter to Delete all tables? ')
            if choice == '':
                testHelper.deleteAllPublicTables(conn)
            if not conn.close:
                conn.close()

    except Exception as detail:
        traceback.print_exc()
import psycopg2
from psycopg2 import sql

def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    """
    Hàm này tạo và trả về một kết nối (connection object) đến cơ sở dữ liệu PostgreSQL thông qua thư viện psycopg2.

    Tham số:
        - user: tên người dùng PostgreSQL (mặc định là 'postgres' – user mặc định khi cài đặt)
        - password: mật khẩu của user PostgreSQL
        - dbname: tên database mà bạn muốn kết nối tới
    Trả về:
        - Một đối tượng kết nối (connection object) đã được mở tới cơ sở dữ liệu PostgreSQL.
    """

    return psycopg2.connect(
        "dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'"
    )

def create_db(dbname):
    """
    Hàm này dùng để tạo cơ sở dữ liệu PostgreSQL mới với tên được truyền vào (vd: 'dds_assgn1') nếu nó chưa tồn tại.

    PostgreSQL không cho phép tạo cơ sở dữ liệu trong một transaction, nên ta phải chuyển kết nối sang chế độ AUTOCOMMIT.

    Thao tác gồm 3 bước chính:
    1. Kết nối đến DB mặc định 'postgres'.
    2. Kiểm tra xem DB cần tạo đã tồn tại chưa.
    3. Nếu chưa có, tạo mới DB.
    """

    # ---------------- BƯỚC 1: KẾT NỐI TỚI DB MẶC ĐỊNH -------------------
    # Kết nối tới database 'postgres' – database hệ thống mặc định
    con = getopenconnection(dbname='postgres')  # dùng 'postgres' để tạo các DB khác

    # Cần bật chế độ tự động commit vì CREATE DATABASE không được phép nằm trong transaction
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    # Tạo cursor để thực thi các câu lệnh SQL
    cur = con.cursor()

    # ---------------- BƯỚC 2: KIỂM TRA SỰ TỒN TẠI -------------------
    # Truy vấn vào hệ thống để xem database tên đã cho đã tồn tại chưa
    cur.execute(
        sql.SQL("SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname = %s"),
        [dbname]
    )

    # Lấy kết quả: nếu count = 0 → database chưa tồn tại
    count = cur.fetchone()[0]

    # ---------------- BƯỚC 3: TẠO DATABASE -------------------
    if count == 0:
        # Tạo database mới với tên đã cho nếu chưa tồn tại
        cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(dbname)))
        print(f"Đã tạo cơ sở dữ liệu mới: {dbname}")
    else:
        # Nếu database đã tồn tại, thông báo cho người dùng
        print(f"Cơ sở dữ liệu '{dbname}' đã tồn tại. Không cần tạo lại.")

    # ---------------- BƯỚC 4: KHÔNG ĐÓNG KẾT NỐI -------------------
    # - Yêu cầu: "Không được đóng kết nối bên trong các hàm đã triển khai"
    # - Test sẽ tự quản lý việc commit và đóng kết nối

def loadratings(ratingstablename, ratingsfilepath, openconnection):
    """
    Hàm này dùng để **nạp dữ liệu từ một file chứa rating (đánh giá phim) vào một bảng PostgreSQL có tên là ratingstablename.

    Tham số:
    - ratingstablename: tên bảng trong database để lưu dữ liệu ratings.
    - ratingsfilepath: đường dẫn tuyệt đối đến file chứa dữ liệu đánh giá (thường có định dạng: userid::movieid::rating::timestamp).
    - openconnection: một đối tượng kết nối đã mở (connection object) tới cơ sở dữ liệu PostgreSQL.

    Lưu ý:
    - Hàm này giả định file đầu vào có định dạng userid::movieid::rating::timestamp, được phân cách bằng dấu ::
    - Schema bảng theo yêu cầu: userid (int), movieid (int), rating (float)
    - Theo yêu cầu: không đóng kết nối, không mã hóa cứng tên file/database
    """

    # Gọi create_db để đảm bảo database đã tồn tại (tên database lấy từ openconnection)
    create_db(openconnection.get_dsn_parameters()['dbname'])

    # -----------------------------------------
    # 1. Tạo con trỏ (cursor) để thực thi các truy vấn SQL trên kết nối đã mở
    # -----------------------------------------
    con = openconnection
    cur = con.cursor()

    # -----------------------------------------
    # 2. Xóa bảng nếu đã tồn tại để tránh conflict khi gọi nhiều lần
    # -----------------------------------------
    cur.execute("DROP TABLE IF EXISTS " + ratingstablename)

    # -----------------------------------------
    # 3. PHƯƠNG PHÁP TỐI ƯU MỚI - NHANH HƠN CODE CŨ:
    # Thay vì tạo bảng với nhiều cột extra rồi ALTER TABLE (chậm):
    # → Ta sẽ pre-process file trước khi copy_from
    # → Tạo luôn bảng với đúng schema cuối cùng
    # → Dùng copy_from với dữ liệu đã được xử lý sẵn
    #
    # Ưu điểm:
    # - Vẫn tận dụng tốc độ siêu nhanh của copy_from
    # - KHÔNG cần ALTER TABLE (tốn kém nhất trong code cũ)
    # - Memory efficient vì xử lý từng dòng, không load hết file
    # -----------------------------------------

    # Tạo bảng chính ngay từ đầu với đúng schema yêu cầu
    cur.execute(
        "CREATE TABLE " + ratingstablename + " (userid INTEGER, movieid INTEGER, rating FLOAT)"
    )

    # -----------------------------------------
    # 4. Tạo StringIO object để chứa dữ liệu đã được xử lý
    # StringIO cho phép ta tạo một "file ảo" trong memory
    # copy_from có thể đọc trực tiếp từ StringIO như đọc file thật
    # -----------------------------------------
    from io import StringIO
    processed_data = StringIO()

    # -----------------------------------------
    # 5. Đọc và xử lý file từng dòng (memory efficient)
    # Chuyển format từ "userid::movieid::rating::timestamp"
    # thành "userid\tmovie_id\trating" (tab-separated cho copy_from)
    # -----------------------------------------
    with open(ratingsfilepath, 'r') as input_file:
        for line in input_file:
            line = line.strip()  # Xóa ký tự xuống dòng và khoảng trắng
            if line:  # Bỏ qua dòng trống
                # Tách dòng theo delimiter '::'
                parts = line.split('::')
                if len(parts) >= 3:  # Đảm bảo có đủ thông tin
                    userid = parts[0]
                    movieid = parts[1]
                    rating = parts[2]
                    # Chỉ lấy 3 trường cần thiết, bỏ qua timestamp (parts[3])
                    # Ghi vào StringIO với tab separator (mặc định của copy_from)
                    processed_data.write(f"{userid}\t{movieid}\t{rating}\n")

    # -----------------------------------------
    # 6. Reset con trỏ StringIO về đầu để copy_from có thể đọc từ đầu
    # -----------------------------------------
    processed_data.seek(0)

    # -----------------------------------------
    # 7. Sử dụng copy_from để import dữ liệu đã xử lý vào bảng
    # copy_from với tab separator (mặc định) → CỰC KỲ NHANH
    # Không cần ALTER TABLE → Tiết kiệm rất nhiều thời gian
    # -----------------------------------------
    cur.copy_from(processed_data, ratingstablename, columns=('userid', 'movieid', 'rating'))

    # -----------------------------------------
    # 8. Đóng StringIO để giải phóng memory
    # -----------------------------------------
    processed_data.close()

    # -----------------------------------------
    # 9. KHÔNG đóng cursor và KHÔNG commit theo yêu cầu
    # - Yêu cầu: "Không được đóng kết nối bên trong các hàm đã triển khai"
    # - Test sẽ tự quản lý việc commit và đóng kết nối
    # -----------------------------------------
    con.commit()
    cur.close()

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm phân mảnh bảng `ratingstablename` thành N phân mảnh ngang theo giá trị cột `rating`,
    tối ưu tuyệt đối về mặt thời gian cho xử lý dữ liệu lớn.

    Parameters:
    ----------
    ratingstablename : str
        Tên bảng gốc chứa dữ liệu đánh giá người dùng.
    numberofpartitions : int
        Số phân mảnh cần tạo (ví dụ: 5).
    openconnection : psycopg2.connection
        Đối tượng kết nối tới PostgreSQL (đã mở, không đóng trong hàm).

    Ý tưởng tối ưu:
    --------------
    - Duyệt đúng 1 vòng `for` để xử lý vừa tạo bảng vừa chèn dữ liệu.
    - Ghép toàn bộ truy vấn SQL thành 1 chuỗi `sql_batch`, gửi 1 lần duy nhất tới DB.
    - Truy vấn `INSERT INTO ... SELECT ... WHERE` giúp PostgreSQL thực hiện tối ưu qua chỉ mục.
    - Tránh tạo bảng tạm, CTE, hoặc nhiều lần gọi `execute()` gây chậm với dữ liệu lớn.

    Hiệu quả:
    ---------
    - Tốc độ xử lý cao nhất với PostgreSQL cho 10 triệu bản ghi trở lên.
    - Dễ mở rộng nếu dùng stored procedure hoặc native partition trong tương lai.
    """

    con = openconnection # Kết nối cơ sở dữ liệu đã mở sẵn
    cur = con.cursor() # Tạo đối tượng thực thi truy vấn SQL

    # Tính khoảng cách giữa các phân mảnh (mỗi phân mảnh bao nhiêu đơn vị rating)
    delta = 5.0 / numberofpartitions  # Độ rộng của mỗi phân mảnh
    RANGE_TABLE_PREFIX = 'range_part'  # Tiền tố tên bảng phân mảnh
    sql_batch = ""  # Ghép toàn bộ câu lệnh SQL để gửi 1 lần duy nhất

    # Duyệt qua từng phân mảnh để tạo bảng và chèn dữ liệu
    for i in range(numberofpartitions):
        min_val = i * delta # Giá trị nhỏ nhất trong phân mảnh thứ i
        max_val = min_val + delta # Giá trị lớn nhất trong phân mảnh thứ i
        table_name = f"{RANGE_TABLE_PREFIX}{i}" # Tên bảng phân mảnh, ví dụ: range_part0

        # 1. Tạo bảng phân mảnh
        sql_batch += f"""
            CREATE TABLE {table_name} (
                userid INTEGER,
                movieid INTEGER,
                rating FLOAT
            );
        """

        # 2. Tạo điều kiện lọc rating phù hợp
        if i == 0:
            # Phân mảnh đầu tiên lấy cả [min, max]
            condition = f"rating >= {min_val} AND rating <= {max_val}"
        else:
            # Các phân mảnh sau: (min, max] để tránh trùng rating biên
            condition = f"rating > {min_val} AND rating <= {max_val}"

        # 3. Thêm câu lệnh chèn vào phân mảnh i
        sql_batch += f"""
            INSERT INTO {table_name} (userid, movieid, rating)
            SELECT userid, movieid, rating
            FROM {ratingstablename}
            WHERE {condition};
        """

    # Thực thi toàn bộ câu SQL cùng lúc (tối ưu tốc độ tối đa)
    cur.execute(sql_batch)
    cur.close() # Đóng cursor
    con.commit() # Lưu thay đổi vào CSDL

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một dòng dữ liệu mới vào bảng chính `ratings` và vào đúng phân mảnh range tương ứng.

    Parameters:
    ----------
    ratingstablename : str
        Tên bảng gốc chứa dữ liệu ratings (ví dụ: "ratings").
    userid : int
        ID của người dùng (user) thực hiện đánh giá.
    itemid : int
        ID của bộ phim được đánh giá (trong hệ thống này itemid chính là movieid).
    rating : float
        Điểm số đánh giá (giá trị từ 0.0 đến 5.0).
    openconnection : psycopg2.connection
        Kết nối đến cơ sở dữ liệu PostgreSQL.

    Mục đích:
    --------
    - Đảm bảo mỗi bản ghi được thêm vào cả bảng chính `ratings` và bảng phân mảnh `range_partX`.
    - Tự động xác định bảng phân mảnh phù hợp dựa trên giá trị `rating`.

    Lưu ý:
    ------
    - Phải chèn **cả hai**: bảng gốc và phân mảnh tương ứng.
    - Nếu `rating` nằm đúng ở biên chia (ví dụ: 2.0), cần chèn vào bảng bên trái để tránh trùng.
    """
    con = openconnection
    cur = con.cursor()

    # -------------------
    # Bước 1: Chèn vào bảng chính `ratings`
    insert_main_table = f"""
            INSERT INTO {ratingstablename} (userid, movieid, rating)
            VALUES ({userid}, {itemid}, {rating});
        """
    cur.execute(insert_main_table)

    # -------------------
    # Bước 2: Tính toán bảng phân mảnh phù hợp để chèn tiếp
    RANGE_TABLE_PREFIX = 'range_part' # Tiền tố bảng phân mảnh
    numberofpartitions = count_partitions(RANGE_TABLE_PREFIX, openconnection) # Đếm số phân mảnh hiện có
    delta = 5.0 / numberofpartitions # Độ rộng của mỗi khoảng phân mảnh

    index = int(rating / delta) # Xác định phân mảnh dựa trên giá trị rating

    # Nếu rating là biên chia (ví dụ: 2.0) và không phải phân mảnh đầu tiên,
    # thì trừ đi 1 để nó thuộc phân mảnh bên trái (đảm bảo không bị trùng)
    if rating % delta == 0 and index != 0:
        index = index - 1
    table_name = RANGE_TABLE_PREFIX + str(index) # Tên bảng phân mảnh cần chèn

    # Chèn dòng vào bảng phân mảnh tương ứng
    cur.execute("insert into " + table_name + "(userid, movieid, rating) values (" + str(userid) + "," + str(itemid) + "," + str(rating) + ");")
    cur.close()
    con.commit() # Lưu thay đổi

def count_partitions(prefix, openconnection):
    """
    Đếm số lượng bảng phân mảnh có tên bắt đầu với prefix trong cơ sở dữ liệu.

    Parameters:
    ----------
    prefix : str
        Tiền tố tên bảng cần đếm (ví dụ: 'range_part', 'rrobin_part')
    openconnection : psycopg2.connection
        Kết nối đến cơ sở dữ liệu.

    Returns:
    -------
    int
        Số lượng bảng phù hợp với tiền tố (tức là số phân mảnh hiện tại).
    """
    con = openconnection
    cur = con.cursor()

    # Truy vấn các bảng người dùng có tên bắt đầu với prefix
    cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + prefix + "%';")
    count = cur.fetchone()[0] # Lấy giá trị đếm ra từ kết quả truy vấn
    cur.close()

    return count # Trả về số bảng phân mảnh

# Mục tiêu: tối ưu hóa thời gian thực thi của quá trình phân mảnh Round-Robin, 
# đặc biệt khi làm việc với các tập dữ liệu lớn như MovieLens 10M dòng.

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    """
    Hàm tối ưu để tạo các phân mảnh của bảng chính bằng phương pháp Round-Robin.
    Hàm này chỉ quét bảng gốc một lần duy nhất và thực hiện các thao tác một cách hiệu quả.
    """
    # Lấy kết nối và con trỏ để thực thi truy vấn
    con = openconnection
    cur = con.cursor()

    # Định nghĩa tiền tố tên bảng cho các phân mảnh round robin để dễ quản lý
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # --- TỐI ƯU 1: Ghép các lệnh DDL (Data Definition Language) vào một chuỗi ---
    # Thay vì thực thi 2*N lệnh CREATE/DROP riêng lẻ, ghép chúng lại và
    # gửi đến server trong một lượt duy nhất để giảm độ trễ mạng.
    create_partition_sql = ""
    for i in range(numberofpartitions):
        # Lệnh xóa bảng phân mảnh cũ nếu nó đã tồn tại để đảm bảo hàm có thể chạy lại nhiều lần.
        create_partition_sql += f"DROP TABLE IF EXISTS {RROBIN_TABLE_PREFIX}{i};\n"
        # Lệnh tạo bảng phân mảnh mới với cấu trúc (schema) được định nghĩa trước.
        create_partition_sql += f"CREATE TABLE {RROBIN_TABLE_PREFIX}{i} (userid INT, movieid INT, rating FLOAT);\n"

    # Thực thi tất cả các câu lệnh tạo/xóa bảng trong một lần gọi duy nhất.
    cur.execute(create_partition_sql)

    # --- TỐI ƯU 2: Sử dụng CTE và Bảng Tạm để quét bảng gốc MỘT LẦN DUY NHẤT ---
    # Gắn số thứ tự dòng (row_num), bắt đầu từ 0, cho mỗi dòng dữ liệu từ bảng gốc.
    # Sử dụng `WITH numbered AS (...)` để thực hiện việc đánh số này.
    # Sau đó, lưu kết quả (bao gồm cả row_num) vào một bảng tạm thời (`TEMP TABLE`)
    # có tên là `temp_numbered_rows`. Bảng tạm này chỉ tồn tại trong phiên làm việc hiện tại.
    # Việc này giúp tránh phải thực thi lại hàm ROW_NUMBER() tốn kém N lần cho N phân mảnh.
    cur.execute(f"""
        WITH numbered AS (
            SELECT userid, movieid, rating,
                   ROW_NUMBER() OVER () - 1 AS row_num
            FROM {ratingstablename}
        )
        SELECT *
        INTO TEMP temp_numbered_rows
        FROM numbered;
    """)

    # --- TỐI ƯU 3: Ghép các lệnh INSERT vào một chuỗi ---
    # Tạo một chuỗi truy vấn lớn chứa tất cả các lệnh INSERT vào từng phân mảnh.
    # Dữ liệu được lấy từ bảng tạm `temp_numbered_rows` đã được tạo ở bước trên.
    insert_all_sql = ""
    for i in range(numberofpartitions):
        # Logic round robin: Dòng nào có (row_num % numberofpartitions) bằng với chỉ số phân mảnh 'i'
        # thì sẽ được chèn vào phân mảnh thứ 'i'.
        # PostgreSQL hỗ trợ hàm MOD(a, b) tương đương với toán tử a % b.
        insert_all_sql += f"""
        INSERT INTO {RROBIN_TABLE_PREFIX}{i} (userid, movieid, rating)
        SELECT userid, movieid, rating FROM temp_numbered_rows
        WHERE MOD(row_num, {numberofpartitions}) = {i};
        """

    # Thực hiện chèn dữ liệu vào tất cả các bảng phân mảnh trong một lần gọi duy nhất.
    cur.execute(insert_all_sql)

    # --- Quản lý Metadata ---
    # Tạo bảng metadata để lưu trữ thông tin cần thiết cho hàm `roundrobininsert`.
    # `partition_number`: lưu chỉ số của phân mảnh gần nhất đã nhận dữ liệu insert.
    # `no_of_partitions`: tổng số phân mảnh đang có.
    cur.execute("DROP TABLE IF EXISTS meta_rrobin_part_info;")
    cur.execute("""
        CREATE TABLE meta_rrobin_part_info (
            partition_number INT,
            no_of_partitions INT
        );
    """)

    # Tính toán chỉ số của phân mảnh cuối cùng đã được chèn dữ liệu.
    cur.execute("SELECT MAX(row_num) + 1 FROM temp_numbered_rows;") # Lấy tổng số dòng đã chèn vào bảng tạm
    total_rows = cur.fetchone()[0] or 0
    last_partition = (total_rows - 1) % numberofpartitions if total_rows > 0 else -1 # Chỉ số phân mảnh cuối cùng đã chèn dữ liệu

    # # Sử dụng truy vấn tham số hóa (%s) để chèn dữ liệu, an toàn hơn ghép chuỗi.
    cur.execute("INSERT INTO meta_rrobin_part_info VALUES (%s, %s);", (last_partition, numberofpartitions))

    # Dọn dẹp: Xóa bảng tạm sau khi đã sử dụng xong để giải phóng tài nguyên trên server.
    cur.execute("DROP TABLE temp_numbered_rows;")

    # Lưu lại tất cả các thay đổi vào cơ sở dữ liệu và đóng con trỏ.
    con.commit()
    cur.close()

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    Hàm chèn một dòng mới vào bảng gốc và vào đúng phân mảnh theo logic Round-Robin.
    Được tối ưu và an toàn hơn.
    """
    # Lấy kết nối và con trỏ để truy vấn
    con = openconnection
    cur = con.cursor()
    RROBIN_TABLE_PREFIX = 'rrobin_part'

    # Bước 1: Lấy thông tin trạng thái phân mảnh hiện tại từ bảng metadata.
    cur.execute("SELECT partition_number, no_of_partitions FROM meta_rrobin_part_info;")
    row = cur.fetchone()
    if row is None:
        raise Exception("Bảng metadata cho Round-Robin chưa được khởi tạo. Vui lòng chạy hàm roundrobinpartition trước.")
    
    partition_number = row[0]  # Số phân mảnh đã insert gần nhất
    no_of_partitions = row[1]  # Tổng số phân mảnh

    # Bước 2: Chèn bản ghi vào bảng gốc (để dữ liệu luôn đầy đủ ở một nơi).
    # SỬ DỤNG TRUY VẤN THAM SỐ HÓA (`%s`) để chèn dữ liệu.
    # Đây là cách thực hành tốt nhất để chống lại SQL Injection và cải thiện hiệu suất.
    cur.execute(
        f"INSERT INTO {ratingstablename} (userid, movieid, rating) VALUES (%s, %s, %s);",
        (userid, itemid, rating)
    )

    # Bước 3: Tính toán chỉ số của phân mảnh tiếp theo theo nguyên tắc round robin.
    # Ví dụ: nếu lần cuối chèn vào phân mảnh 0 và có 5 phân mảnh,
    # (0 + 1) % 5 = 1. Lần chèn tiếp theo sẽ vào phân mảnh 1.
    next_partition = (partition_number + 1) % no_of_partitions
    partition_table_name = f"{RROBIN_TABLE_PREFIX}{next_partition}"

    # Bước 4: Chèn bản ghi vào bảng phân mảnh tương ứng.
    # Tiếp tục sử dụng truy vấn tham số hóa để đảm bảo an toàn.
    cur.execute(
        f"INSERT INTO {partition_table_name} (userid, movieid, rating) VALUES (%s, %s, %s);",
        (userid, itemid, rating)
    )

    # Bước 5: Cập nhật trạng thái partition vừa insert vào bảng metadata.
    # SỬ DỤNG LỆNH `UPDATE` thay vì `TRUNCATE` + `INSERT`.
    # Đối với một bảng chỉ có một dòng, `UPDATE` thường hiệu quả hơn vì nó
    # chỉ sửa đổi dữ liệu tại chỗ thay vì xóa toàn bộ bảng rồi chèn lại.
    cur.execute("UPDATE meta_rrobin_part_info SET partition_number = %s;", (next_partition,))

    # Bước 6: Lưu tất cả các thay đổi (2 lệnh INSERT và 1 lệnh UPDATE) vào cơ sở dữ liệu và đóng con trỏ.
    con.commit()
    cur.close()

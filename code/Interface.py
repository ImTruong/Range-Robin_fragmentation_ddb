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
    Hàm tạo các phân mảnh (partitions) của bảng `ratingstablename` dựa trên giá trị của cột `rating`.

    Parameters:
    ----------
    ratingstablename : str
        Tên bảng chứa dữ liệu đánh giá (ratings) gốc, ví dụ: "ratings".
    numberofpartitions : int
        Số lượng phân mảnh cần tạo (ví dụ: 5).
    openconnection : psycopg2.connection
        Đối tượng kết nối đến cơ sở dữ liệu PostgreSQL.

    Mục đích:
    --------
    - Chia bảng `ratings` thành các bảng nhỏ hơn `range_part0`, `range_part1`, ..., `range_part{N-1}`
      dựa trên các khoảng giá trị đồng đều của cột `rating`.
    - Dải rating giả định trong khoảng [0, 5].

    Lưu ý:
    ------
    - Tên bảng phân mảnh phải bắt đầu bằng `range_part` để tương thích với bộ kiểm thử.
    - Không được trùng lặp dữ liệu giữa các phân mảnh (tức là mỗi dòng chỉ thuộc một phân mảnh).
    """
    con = openconnection # Kết nối cơ sở dữ liệu đã mở sẵn
    cur = con.cursor() # Tạo đối tượng thực thi truy vấn SQL

    # Tính khoảng cách giữa các phân mảnh (mỗi phân mảnh bao nhiêu đơn vị rating)
    delta = 5 / numberofpartitions  # Nếu có 5 phân mảnh thì delta = 1.0
    RANGE_TABLE_PREFIX = 'range_part' # Tiền tố tên bảng phân mảnh

    # Duyệt qua từng phân mảnh để tạo bảng và chèn dữ liệu
    for i in range(0, numberofpartitions):
        minRange = i * delta # Giá trị nhỏ nhất trong phân mảnh thứ i
        maxRange = minRange + delta # Giá trị lớn nhất trong phân mảnh thứ i
        table_name = RANGE_TABLE_PREFIX + str(i) # Tên bảng phân mảnh, ví dụ: range_part0

        # Tạo bảng phân mảnh mới với 3 cột chính: userid, movieid, rating
        cur.execute("create table " + table_name + " (userid integer, movieid integer, rating float);")

        # Chèn dữ liệu từ bảng gốc vào bảng phân mảnh theo điều kiện rating
        if i == 0:
            # Phân mảnh đầu tiên chứa cả biên trái và phải: [min, max]
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(minRange) + " and rating <= " + str(maxRange) + ";")
        else:
            # Các phân mảnh còn lại chỉ chứa (min, max]: loại trừ biên trái
            cur.execute("insert into " + table_name + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(minRange) + " and rating <= " + str(maxRange) + ";")
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
    delta = 5 / numberofpartitions # Độ rộng của mỗi khoảng phân mảnh

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

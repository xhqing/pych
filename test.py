from pych import PyCH

conn = PyCH(host="", username="", password="", port="", dbname="", send_receive_timeout=2000)

df = conn.get_pandasDataFrame("""
    select
      *
    from
      default.gps_info_cluster
    limit
      5
""")
print(df)

df.loc[0, "gps_id"] = "test"
df.loc[1, "gps_id"] = "test"

conn.write_pandasDataFrame(df=df, tablename="default.gps_info_cluster")

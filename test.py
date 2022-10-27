from pych import PyCH

conn = PyCH(host="", dbname="", username="", password="", port="", send_receive_timeout=2000)

df = conn.get_pandasDataFrame("""
    select
      *
    from
      gps_info_cluster
    limit
      5
""")
print(df)

df.loc[0, "gps_id"] = "test"
df.loc[1, "gps_id"] = "test"

conn.write_pandasDataFrame(df=df, tablename="gps_info_cluster")

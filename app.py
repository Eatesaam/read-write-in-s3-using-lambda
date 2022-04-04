import io
import boto3
import pandas as pd
import xml.etree.ElementTree as ET

def lambda_handler(event, context):
    print("Mapping Device Data")
    # cretaing s3 session
    s3 = boto3.client('s3')
    # bucket name
    bucket = "pe-data-input-sources"
    # meter reading key
    meter_readings_s3_path = "aprose/2021-06-26/2021_06_26_Weekly 8F0A.csv"
    # device report key
    device_report_s3_path = "aprose/2021-06-26/2021_06_26_Device Report with Complete.csv"
    # reading meter reading csv file from s3
    data = s3.get_object(Bucket=bucket, Key=meter_readings_s3_path)
    meter_reading_csv_data = pd.read_csv(data["Body"])
    # reading device report csv file from s3
    data = s3.get_object(Bucket=bucket, Key=device_report_s3_path)
    device_report_csv_data = pd.read_csv(data["Body"])
    # group by Type and get GPF group and convert DeviceID to list
    gpf_data_list = meter_reading_csv_data.groupby("Type").get_group("GPF")["DeviceID"].tolist()
    gpf_comm_list = meter_reading_csv_data.groupby("Type").get_group("GPF")["commisioned_date"].tolist()
    # getting unmaped device id
    device_id_log = []
    # get mxpn from device report csv
    for data,comm in zip(gpf_data_list,gpf_comm_list):
        df = device_report_csv_data[(device_report_csv_data['GPFID'] == data) & (device_report_csv_data['MPXN_Number'].notna())]
        df_list = df.values.tolist()
        if len(df_list) == 0:
            device_id_log.append((data,"GPF",comm))
        elif len(df_list) > 0:
            for df_ls in df_list:
                mpxn = df_ls[2]
            meter_reading_csv_data.loc[meter_reading_csv_data['DeviceID'] == data, 'MPXN_Number'] = mpxn
    # print unmapped device id that not exists in device report
    # print(f"unmapped device id: {device_id_log}")
    # drop row which mpxn does not exists
    meter_reading_csv_data.dropna(subset=['MPXN_Number'], inplace = True)
    # convert meter reading dataframe to list
    meter_reading_list = meter_reading_csv_data.values.tolist()
    # meter readings list
    meter_readings = []
    # device meter reading not exists
    device_meter_undefiend = []
    # getting tariff from xml of Response column
    for data in meter_reading_list:
        clean_xml = data[5].replace("version=1.0\"", "version=\"1.0\"")
        clean_xml = clean_xml.replace(">\"", ">")
        root = ET.fromstring(clean_xml)
        nsmap = {'': 'http://www.dccinterface.co.uk', 'ra':'http://www.dccinterface.co.uk/ResponseAndAlert'}
        timestamp = root.find(".//ra:Timestamp", namespaces=nsmap).text
        readings = root.findall(".//ra:TariffTOURegisterMatrixValue", namespaces=nsmap)
        reading_rate = []
        for reading in readings:
            reading_rate.append(reading.text)
        # check condition
        if len(reading_rate) == 0:
            device_meter_undefiend.append((data[0],data[2],data[3]))
            reading_rate_1 = 0
            reading_rate_2 = 0
        else:
            reading_rate_1 = reading_rate[0]
            reading_rate_2 = reading_rate[1]
        # generate meter reading record
        meter_reading = (data[0],data[1],data[2],timestamp,float(reading_rate_1),float(reading_rate_2))
        # appen meter reading record in list
        meter_readings.append(meter_reading)
    
    # print(f"meter_readings: {meter_readings}")

    # print(f"meter reading not found: {device_meter_undefiend}")
    # write devices with no meter reading in csv file in s3 bucket
    meter_reading_undefined_path= "aprose/2021-06-26/devices_with_no_meter_readings.csv"
    meter_reading_undefined_data = pd.DataFrame(device_meter_undefiend, columns=['Device ID', 'Type', 'Commissioned Date'])
    with io.StringIO() as csv_buffer:
        meter_reading_undefined_data.to_csv(csv_buffer, index=False)
        response = s3.put_object(Bucket=bucket,Key=meter_reading_undefined_path,Body=csv_buffer.getvalue())
    # write devices with no mpxn number in csv file in s3 bucket
    device_id_undefined_path= "aprose/2021-06-26/devices_with_no_mpxn.csv"
    device_id_undefined_data = pd.DataFrame(device_id_log, columns=['Device ID', 'Type', 'Commissioned Date'])
    with io.StringIO() as csv_buffer:
        device_id_undefined_data.to_csv(csv_buffer, index=False)
        response = s3.put_object(Bucket=bucket,Key=device_id_undefined_path,Body=csv_buffer.getvalue())
    # write meter readings in csv file in s3 bucket
    meter_reading_path= "aprose/2021-06-26/mpxn_with_meter_readings.csv"
    meter_reading_data = pd.DataFrame(meter_readings, columns=['Device ID', 'MPXN', 'Type', 'Timestamp', 'Reading 1', 'Reading 2'])
    with io.StringIO() as csv_buffer:
        meter_reading_data.to_csv(csv_buffer, index=False)
        response = s3.put_object(Bucket=bucket,Key=meter_reading_path,Body=csv_buffer.getvalue())

    return {
        "Bucket": bucket,
        "Meter Reading With Mxpn Path": meter_reading_path,
        "Devices With No Mpxn Path": device_id_undefined_path,
        "Devices With No Meter Reading Path": meter_reading_undefined_path
    }

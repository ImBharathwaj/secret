import os
import ftplib

FTP_HOST = 'ftp.dlptest.com'
FTP_USER = 'dlpuser'
FTP_PASS = 'rNrKYTX9g7z3RgJRmxWuGHbeu'

ftp = ftplib.FTP(FTP_HOST, FTP_USER, FTP_PASS)
ftp.encoding = "utf-8"

def egress():
    filename = "filename.csv"
    with open(filename, "rb") as file:
        # use FTP's STOR command to upload the file
        ftp.storbinary(f"STOR {filename}", file)

def ingress():
    filename = "filename.csv"
    with open(filename, "wb") as file:
        # use FTP's RETR command to download the file
        ftp.retrbinary(f"RETR {filename}", file.write)

if __name__ == '__main__':
    print(ftp.dir())
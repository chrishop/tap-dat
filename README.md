# tap-dat
A tool for downloading 1 million+ data points from an astronomical database using the TAP protocol

# Install
```
chmod +x install.sh
./install.sh
```
Before you set this up you need to install mysql server
```
sudo apt update
sudo apt install mysql-server

# starts sql
sudo systemctl start mysql

# makes the sql server launch at reboot
sudo systemctl enable mysql

# enter into database
/usr/bin/mysql -u root -p

```

# Usage
creates sample json config file:
```
tap-dat init
```

You can then edit this config file with a text editor of your choice!
This is a sample config file that fetches 100 rows in batches of 10 from dr1.master in the skymapper TAP database:
```
{
    "remote_url": "http://api.skymapper.nci.org.au/public/tap",
    "remote_table": "dr1.master",
    "remote_table_id": "object_id",
    "remote_table_id_min": 1,
    "remote_table_id_max": 100,
    "download_batch_size": 10,
    "local_host": "localhost",
    "local_user": "root",
    "local_password": "*******",
    "local_db_name": "tap_dat_test",
    "local_table_name": "quick_test"
}
```
To initiate the downloading run:
```
tap-dat run tap-dat.conf
```
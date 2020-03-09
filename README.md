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
```
tap-dat init
vim tap-dat.conf (edit the json to have the correct parameters)
tap-dat run tap-dat.conf
```
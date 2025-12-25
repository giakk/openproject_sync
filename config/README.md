# Useful Commands for Installation

## Clone and Setup for the Python Project

```bash
git clone https://github.com/giakk/openproject_sync.git
cd openproject_sync
```

Create the Python virtual environment:

```bash
python3 -m venv venv
source venv/bin/activate
```

Install all required packages from the `requirements.txt` file:

```bash
pip install -r requirements.txt
```

#### Install the Microsoft Driver for SQL Server on Ubuntu

```bash
sudo apt-get update
sudo apt-get install -y unixodbc unixodbc-dev

sudo apt-get install -y odbcinst odbcinst1debian2 libodbc1

curl https://packages.microsoft.com/keys/microsoft.asc | sudo apt-key add -
curl https://packages.microsoft.com/config/ubuntu/22.04/prod.list | sudo tee /etc/apt/sources.list.d/mssql-release.list
sudo apt-get update
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

---

## PostgreSQL 17 Installation Guide

```bash
sudo apt update && sudo apt upgrade -y

sudo apt install -y wget ca-certificates software-properties-common \
  apt-transport-https lsb-release

wget -qO- https://www.postgresql.org/media/keys/ACCC4CF8.asc \
  | sudo gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg

echo "deb [arch=amd64,arm64,ppc64el] \
http://apt.postgresql.org/pub/repos/apt/ \
$(lsb_release -cs)-pgdg main" \
| sudo tee /etc/apt/sources.list.d/postgresql.list

sudo apt update
sudo apt install -y postgresql-17 postgresql-client-17 postgresql-contrib-17
```

Verify the installation:

```bash
sudo systemctl status postgresql
psql --version
```

## PostgreSQL Setup Guide

### Set a Password for the PostgreSQL Superuser

```bash
sudo -u postgres psql
```

```sql
ALTER USER postgres PASSWORD 'your_new_password';
```

### Create a Database for OpenProject

First, create a dedicated user that will own the database and have limited privileges compared to the `postgres` superuser.
Save the password securely and store it in the configuration file.

```bash
sudo -u postgres createuser --interactive --pwprompt openproject
sudo -u postgres createdb -O openproject openproject_db
```

### Create the Cache Database for Synchronization

```bash
sudo -u postgres createdb -O openproject sync_cache_db
```

Create the tables:

```bash
psql -h localhost -U openproject -d sync_cache_db -a \
  -f ../app/queries/create_tables.sql
```

---

## OpenProject Installation and Setup Guide

### Installation and Initial Setup

```bash
sudo apt-get update
sudo apt-get install -y apt-transport-https ca-certificates wget
```

```bash
sudo wget -O /etc/apt/trusted.gpg.d/openproject.asc \
  https://dl.packager.io/srv/opf/openproject/key
```

```bash
sudo wget -O /etc/apt/sources.list.d/openproject.list \
  https://dl.packager.io/srv/opf/openproject/stable/16/installer/ubuntu/22.04.repo
```

```bash
sudo apt-get update
sudo apt-get install -y openproject
```

```bash
sudo openproject configure
```

**Note:** For the fully qualified domain name (FQDN), use the VM IP address.
You can retrieve it using the `ipconfig` or `ip a` command.

### Plugin Setup

```bash
sudo cp ./Gemfile.custom /etc/openproject/
```

```bash
sudo chown openproject:openproject /etc/openproject/Gemfile.custom
sudo chmod 644 /etc/openproject/Gemfile.custom
```

```bash
sudo openproject config:set CUSTOM_PLUGIN_GEMFILE="/etc/openproject/Gemfile.custom"
sudo openproject config:set RECOMPILE_ANGULAR_ASSETS="true"
```

```bash
sudo openproject configure
```

### Useful command for Microsoft SQL Server
If you need to create a user and a login to access the database through the python scritp, you'll need these commands:
```sql
-- 1. Create login at server level
CREATE LOGIN lettore_readonly WITH PASSWORD = 'Password123!';
GO

-- 2. Set the database
USE nome_database;
GO

-- 3. Create user mapped to the login
CREATE USER lettore_readonly FOR LOGIN lettore_readonly;
GO

-- 4. Assign read only access to the user
ALTER ROLE db_datareader ADD MEMBER lettore_readonly;
GO
```

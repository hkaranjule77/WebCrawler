# WebCrawler
Surfs Internet through scraping links. It is built with python language and utilize SQL to maintain data.

## Features
- Scrapes valid link only.
- Stores a copy of a Webpage.
- Supports Multi-threading with optimized performance.
- Maintains consistency of the database.
- Varied maximum link storage limit.
- Refreshs content after specified interval of time.

## Software Requirements  
- Git 
- Python (3.x.x)
- pip (Python Package Manager)
- MySQL Server
- Python Environment Manager (conda / pipenv / virtualenv)

### Third Party Python Libraries  
- BeautifulSoup
- mysql_connector_python
- requests

## Installation
### Linux (Debian/Ubuntu)
To update packages on your PC:  
```
sudo apt update
```
To install Git run following command on terminal:
```
sudo apt install git
```

To install Python run following command on terminal:  
```
sudo apt install python3
```  
  
To install MySQL Server run following command on terminal:  
```
sudo apt install mysql-server
```

> For other distributions of Linux, like Fedora/Red Hat/CentOS replace **apt** keyword with 
> **rpm** in above commands.

### Windows

Visit following links to download and install Git:
- [Git Downloads](https://git-scm.com/download/)

Visit following links to download and install Python 3:
- [Installation Guide](https://docs.python.org/3/using/windows.html)
- [Python Downloads](https://www.python.org/downloads/)

Visit following links to download and install MySQL:
- [Installation Guide](https://dev.mysql.com/doc/refman/8.0/en/windows-installation.html)  
- [MySQL Installer Page](https://dev.mysql.com/downloads/installer/)

### Anaconda Package Manager
You can skip this step if you don't have any plans to work with Python.  
Or visit following hyperlink to know more about how to install Anaconda on various Operating System.
- [Installation Guide](https://docs.anaconda.com/anaconda/install/index.html)
  
After installation, run following commands in terminal/CMD to create new environment: 
```
conda create -n crawler
```

To activate newly created environment named crawler run following command:  
```
conda activate crawler 
```

### Cloning the Git Repository
Execute following command on `termainal/CMD` to clone above repository:
```
git clone https://github.com/hkaranjule77/WebCrawler.git
```

Then change directory into WebCrawler. 
```
cd WebCrawler
```

### Python Third-party Package 
Following command will install all required third-party packages.
```
pip install -r requirements.txt
```
## Execution
For Linux
```
python3 main.py
```  
For Windows
```
python main.py
```

## Contributor
**Harshad Karanjule**
- [GitHub](https://github.com/hkaranjule77)
- [LinkedIn](https://www.linkedin.com/in/harshad-karanjule-5b076818b/)

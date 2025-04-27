
Manual Setup and Run
1. Install Python 3.8 or 3.9 from https://www.python.org/downloads/. Add Python and pip to PATH.
2. Verify Python version:
   - Open Command Prompt (cmd) and run: python --version
   - It should show 3.8 or 3.9.
3. Install JDK 8:
   - Download JDK 8 from https://adoptopenjdk.net/?variant=openjdk8.
   - Install to C:\Program Files\Java\jdk-8.0.452.
   - Set JAVA_HOME: In cmd, run: setx JAVA_HOME "C:\Program Files\Java\jdk-8.0.452"
   - Add to PATH: setx PATH "%PATH%;C:\Program Files\Java\jdk-8.0.452\bin"
   - Verify: java -version (should show version 1.8.x).
4. Install Winutils for PySpark:
   - Create folder C:\hadoop\bin.
   - Download winutils.exe from https://github.com/cdarlint/winutils/raw/master/hadoop-2.7.1/bin/winutils.exe.
   - Place it in C:\hadoop\bin.
   - Set HADOOP_HOME: setx HADOOP_HOME "C:\hadoop"
   - Add to PATH: setx PATH "%PATH%;C:\hadoop\bin"
5. Create virtual environment:
   - In cmd, run: python -m venv C:\sales_env
6. Activate virtual environment:
   - Run: C:\sales_env\Scripts\activate
7. Create data directory:
   - Create folder C:\Projects\data.
8. Copy files:
   - Copy sales_analysis.py to C:\sales_env.
   - Copy superstore_data_extended.csv to C:\Projects\data.
9. Install dependencies:
   - Create requirements.txt in C:\Projects\data with:
     pyspark==3.2.0
     pandas==1.5.3
     plotly==5.22.0
     networkx==3.3
     scikit-learn==1.5.0
     xgboost==2.0.3
     scipy==1.13.1
     fpdf==1.7.2
   - Run: pip install -r C:\Projects\data\requirements.txt
10. Run the project:
    - In cmd (with virtual environment activated), run: cd C:\sales_env
    - Then: python sales_analysis.py
11. Check outputs in C:\Projects\data (PDF report). HTML dashboards are on Google Drive (see README.md).
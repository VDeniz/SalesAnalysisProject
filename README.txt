Manual Setup and Run
1. Install Python 3.11 from https://www.python.org/downloads/.
   - Download Python 3.11.9 (recommended).
   - During installation, check "Add Python 3.11 to PATH".
   **Change: Updated Python version to 3.11 to match your system.**

2. Verify Python version:
   - Open Command Prompt (cmd) and run: python --version
   - It should show 3.11.x (e.g., 3.11.9).

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

7. Install dependencies:
   - Ensure requirements.txt is in C:\sales_env with the following:
     ```
     pyspark==3.2.0
     pandas==1.5.3
     plotly==5.22.0
     networkx==3.3
     scikit-learn==1.5.0
     xgboost==2.0.3
     scipy==1.13.1
     fpdf==1.7.2
     ```
   - Run: pip install -r C:\sales_env\requirements.txt
   - Note: If you encounter a "numpy.dtype size changed" error, install a compatible numpy version:
     pip install numpy==1.24.3 --force-reinstall
   **Change: Added numpy compatibility note based on previous error.**

8. Prepare directory structure:
   - Create the following folders if they don't exist:
     - C:\sales_env\src
     - C:\sales_env\data
     - C:\sales_env\data\input
     - C:\sales_env\data\output
   - Copy superstore_data_extended.csv to C:\sales_env\data\input.
   **Change: Explicitly added creation of src, data, and output folders.**

9. Copy files:
   - Ensure sales_analysis.py is in C:\sales_env\src.

10. Run the project:
    - In cmd (with virtual environment activated), run: cd C:\sales_env\src
    - Then: python sales_analysis.py

11. Check outputs in C:\sales_env\data\output:
    - Sales_Analysis_Report.pdf: Detailed report (opens automatically after execution).
    - xgboost_model_step3.pkl: Trained XGBoost model.
    - key_categories_graph.html: Interactive graph and query results.
    - Chart.html: Interactive dashboards.
    **Change: Noted that PDF opens automatically (assuming you add the code below).**

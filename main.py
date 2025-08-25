#C:\Users\kevin\Documents\DINAMIC 2\secretaria_ecuador\CanalCerrado_Facebook_BancoGuayaquil-main\main.py
import concurrent.futures
import subprocess

def run_script(script_name):
    # This function will run the script
    try:
        subprocess.run(["python", script_name], check=True)
    except subprocess.CalledProcessError as e:
        print(f"An error occurred while running {script_name}: {e}")

def main():
    scripts = ["posts_bg.py", "comments_bg.py"]

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Use the executor to run both scripts concurrently
        futures = [executor.submit(run_script, script) for script in scripts]
        
        # Wait for both scripts to complete
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # This will re-raise any exceptions that occurred
            except Exception as e:
                print(f"An error occurred: {e}")

if __name__ == "__main__":
    main()

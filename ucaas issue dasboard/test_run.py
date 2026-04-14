import subprocess
res = subprocess.run([r"C:\Users\mcm\AppData\Local\Programs\Python\Python314\python.exe", "manage.py", "runserver", "0.0.0.0:8000", "--noreload"], capture_output=True, text=True)
with open("test_out.txt", "w") as f:
    f.write("STDOUT:\n")
    f.write(res.stdout)
    f.write("\nSTDERR:\n")
    f.write(res.stderr)
    f.write(f"\nRETURN CODE: {res.returncode}\n")

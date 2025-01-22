import sys
import subprocess

if __name__ == '__main__':
    # Get the number of times from command-line argument
    if len(sys.argv) < 2:
        print("Usage: python3 run_multiple.py <num_runs>")
        sys.exit(1)
    num_runs = int(sys.argv[1])

    processes = []
    cmd = ["python3", "kafka_collect_produce.py"]

    # Loop to run the script multiple times   
    try:
        # Run multiple instances of the metric collector script concurrently
        for i in range(num_runs):
            print(f"Running script iteration: {i}")
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(process)
        # Wait for user input to stop
        input("Press Enter to stop.")
    except KeyboardInterrupt:
        print("Keyboard Interrupt, exiting...")
    finally:
        # Terminate subprocesses
        for process in processes:
            process.terminate()
        print("All script instances terminated.")


import subprocess
import logging
import os
import docker
import time

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('scheduler.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_docker_command():
    """Chạy lệnh Docker và đợi nó hoàn thành"""
    try:
        logger.info("Running Docker command: make run-scaled")
        # Chạy lệnh make run-scaled trong background
        process = subprocess.Popen("make run-scaled", shell=True)
        
        # Đợi 30 giây để Docker container khởi động
        time.sleep(30)
        
        # Kiểm tra xem container có đang chạy không
        client = docker.from_env()
        containers = client.containers.list()
        if not containers:
            logger.error("No Docker containers running")
            return False
            
        logger.info("Docker container is running")
        return True
    except Exception as e:
        logger.error(f"Error running Docker command: {e}")
        return False

def run_make_command(command):
    """Chạy lệnh make thông thường"""
    try:
        logger.info(f"Running command: {command}")
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        logger.info(f"Command output: {result.stdout}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error running command: {e}")
        logger.error(f"Error output: {e.stderr}")
        return False

def main():
    """Thực thi job"""
    logger.info("Starting job execution")
    
    # Lấy đường dẫn hiện tại
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(os.path.dirname(current_dir))
    
    # Chuyển đến thư mục gốc của project
    os.chdir(project_root)
    
    # Chạy lệnh Docker trước
    if not run_docker_command():
        logger.error("Failed to run Docker command")
        return
    
    # Đợi thêm 30 giây để đảm bảo Docker đã sẵn sàng
    time.sleep(30)
    
    # Chạy lệnh make submitmain
    if not run_make_command("make submitmain"):
        logger.error("Failed to run make submitmain")
        return
    
    logger.info("Job completed successfully")

if __name__ == "__main__":
    main() 
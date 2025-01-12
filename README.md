# AWS-Backup-Manager
Developed a Python application leveraging AWS S3 and Boto3 to enable seamless cloud-based backup and restore functionality, ensuring efficient file management and directory traversal.

## Backup Command

To perform a backup, use the following command:

```bash
python3 backup.py backup <directory> bucket::<directory>
```
To perform a restore use the following command:
```bash
python3 restore.py restore bucket::<directory> <directory>
```

1. frequency of files and size--help us for scanning improvments
2. schedule or near to real time


m assuming all files will be in same folder --now the thing is how to differnciate files
arheicture one
previous job should be completed then only new jobs should trigger --
thr should we a way to restart a job-
overwright only option left for same file--in db it should be upsert
use last modified time stamp--in same s3 but in differnt folder may be--cheap or if not then dynamodb
input/files/--
archieve
and move all the files 
💸 Cost advantage

Example:

Scenario	Files Scanned	API Calls
No archive	1,000,000	~1000
With archive	100	~1

moving files from one to another 

the last thing is job failure

In AWS Glue terms:

👉 Trigger = rule + condition → start execution

s3://customer-metadata/
    ├── watermark/
    │     └── watermark.json
    └── batches/
          ├── batch_001.json
          ├── batch_002.json

git init
git add README.md
git commit -m "first commit"
git branch -M main
git remote add origin https://github.com/sourav4994/awsglue-workflow-understanding.git
git push -u origin main
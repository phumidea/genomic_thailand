# genomic_thailand
this is a part of senior project from phum lertritmahachai

คู่มือสำหรับการติดตั้งและใช้งาน data preparation สำหรับ สวทช

ใน deliver จะประกอบด้วย file 3 ส่วน ได้แก่

1) installation สำหรับการติดตั้ง library และการกำหนด environment ต่างๆสำหรับการใช้งาน
2) code for demo เป็น source code สำหรับการทดสอบผลลัพธ์ ซึ่งมีการ minimal บางคำสั่ง เพื่อเป็นการ proof of concept แนวคิดในการทำงาน
3) code for production เป็น source code สำหรับการใช้งานบน production จริงๆ

############# installation ##############
ภายใน installation จะประกอบไปด้วย file ต่างๆดังนี้
1) Miniconda3-latest-Linux-x86_64.sh	สำหรับการติดตั้ง conda environment ช่วยในการติดตั้ง library และ package ได้ง่ายขึ้น
2) environment.yml			สำหรับการระบุว่าต้องติดตั้ง package อะไรบ้าง โดยมีการกำหนด version ของ library สำหรับใช้คู่กับ anaconda

หมายเหตุ ผู้ใช้งานต้อง download miniconda เองเนื่องจาก file ีขนาดใหญ่เกิน 25 MB

วิธีการติดตั้ง
1) apt-get update && apt-get -y upgrade
2) ทำการย้าย file ไปยัง
2) curl -O https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
3) sh Miniconda3-latest-Linux-x86_64.sh => yes => enter => yes => bash
4) conda config --add channels defaults
5) conda config --add channels bioconda
6) conda config --add channels conda-forge
7) conda install -y bwa
8) conda env update -f environment.yml => ask environment from Phum
9) apt install -y openjdk-8-jre-headless 
10 pip3 install cassandra-driver

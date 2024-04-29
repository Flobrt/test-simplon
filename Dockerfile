# Docker file for building the image for python script
FROM python:3.8

# Set the working directory
WORKDIR /usr/src/app

# Copy the python script to the container
COPY hello-world.py /usr/src/app/
# Copy the requirements file to the container
COPY requirements.txt /usr/src/app/

# Install the required python packages
RUN pip install --no-cache-dir --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt


# Run the python script
CMD ["python", "hello-world.py"]



# Step 1: Set the base image for Node.js
FROM node:alpine3.18

# Step 2: Set the working directory in the container
WORKDIR /app

# Step 3: Copy package.json and package-lock.json to the working directory
COPY package.json package-lock.json ./

# Step 4: Install dependencies
RUN npm install

# Step 5: Copy the rest of the application code to the working directory
COPY . .

# Step 6: Expose the application port
EXPOSE 8098

# Step 7: Define the command to run the application
CMD ["npm", "start"]

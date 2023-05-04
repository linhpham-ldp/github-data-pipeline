# Step-by-step instructions on reproducing the project

## **Infrastructure set-up**
### Prerequisites
The following requirements are needed to reproduce the project:

1. A [Google Cloud Platform](https://cloud.google.com/) account
1. (Optional) The [Google Cloud SDK](https://cloud.google.com/sdk). Instructions for installing it are below.
    * Most instructions below will assume that you are using the SDK for simplicity.
1. (Optional) A SSH client
    * All the instructions listed below assume that you are using a Terminal and SSH.
1. (Optional) VSCode with the Remote-SSH extension
    * Any other IDE should work, but VSCode makes it very convenient to forward ports in remote VM's.
<br>
### Step 1: Set up a Google Cloud project 
We should be able to create a new project and its corresponding service account.
<details>
  <summary>For more details, click here</summary>
    
  > 1. Create a GCP **New Project** 

  > ![purchase-insights-pipeline](.images/gcp_setup_step01.png)
  >
  > 2. **Switch** to the project
  > 3. Go to *IAM & Admin / Service Account /* **Create service account**
  > 4. *Service account details* --> set *Service account name* (it's up to you) --> **Create and continue**
  >
  > ![purchase-insights-pipeline](.images/gcp_setup_step02.png)
  >
  > 5. In *Grant this service account access to project* ---> add the following roles:
  >    - Add **Basic / Viewer** *(optional)*
  >    - Add **Cloud Storage / Storage Admin**
  >    - Add **Cloud Storage / Storage Object Admin**
  >    - Add **BigQuery Admin**
  >    --> Click "Done"
  >
  > ![purchase-insights-pipeline](.images/gcp_setup_step03.png)
  > 
  > 6. Go to *Service Accounts / 3 dots under Actions* --> **Manage Keys**
  >
  > ![purchase-insights-pipeline](.images/gcp_setup_step04.png)
  >
  > 7. Go to *Add key /* **Create new key** --> Choose **JSON** format
  >
  > ![purchase-insights-pipeline](.images/gcp_setup_step05.png)
  > 
  > A json file will be downloaded to a default folder in your local machine (It's the Downloads folder for me). Let's remember the path to the file as we will need it in the next step.
</details>
<br>

### Step 2: Set up a VM instance on GCP

We want to run and deploy our data pipeline via a virtual machine on Google Cloud Platform (GCP). This will allow us to leverage Google's high-performance infrastructure (with cost but we have the free $300 credit granted to a new account).

<details>
  <summary>For more details, click here</summary>
    
  > 1. Go to *Compute Engine / Settings / Metadata* --> Make sure the public SSH key is added (follow [Google Cloud instructions here](https://cloud.google.com/compute/docs/connect/create-ssh-keys)) 
  > 2. Go to *Compute Engine / VM instances /* --> Enable **Compute Engine** API (this step is not needed is the API has already been enabled, you will see manage instead)
  >
  > ![purchase-insights-pipeline](.images/gcp_vm_step01.png)
  >
  > 3. Under *VM instances /* --> **Create instance**
    >
  > ![purchase-insights-pipeline](.images/gcp_vm_step02.png)
  >
  > 4. In *Grant this service account access to project* ---> add the following roles:
  >    - Name = your choice 
  >    - Region, Zone = select a region near you/I used the default one for Zone
  >    - Machine Type = 4vCPu, 16 GB Memory (e2-standard-4)
  >    - Boot Disk:
  >         - Select Ubuntu and Ubuntu 20.04 LTS (x86/64) as the Operating System and Version
  >         - Size = 30 GB
  >
  > ![purchase-insights-pipeline](.images/gcp_vm_step03.png)
  >
  > 6. Click "CREATE"

</details>
### Step 3: Install Anaconda 
Command: wget https://repo.anaconda.com/archive/Anaconda3-2023.03-Linux-x86_64.sh
 To use Anaconda: bash Anaconda3-2023.03-Linux-x86_64.sh

---

## **Data Pipeline**

### Step 1: Data ingestion

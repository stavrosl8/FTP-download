from datetime import datetime
import pandas as pd
import numpy as np
import ftplib
import tqdm
import re

def Finding_the_path (date_idx):
    dates = re.split("[/  :]",date_idx)  
    year,month,day,hour = dates[0], dates[1], dates[2], dates[3] 
    ftp.cwd(path_ftp+year)  
    img_list = ftp.nlst()
    flag_data = False
    if month in img_list:
        ftp.cwd(path_ftp+year+"/"+month)  
        img_list = ftp.nlst()
        if day in img_list:
            ftp.cwd(path_ftp+year+"/"+month+"/"+day)  
            img_list = ftp.nlst()           
            if (len(img_list)>33):
                flag = "Folders with hours are missing but the data exist"
                flag_data = True
                flag = "Available Data"
            elif (len(img_list)==2):
                flag = "The daily folder is empty"
            else:                
                if hour in img_list:
                    ftp.cwd(path_ftp+year+"/"+month+"/"+day+"/"+hour)  
                    img_list = ftp.nlst()
                    flag_data = True
                    flag = "Available Data"
                else:
                    flag = "The hourly folder is missing"
        else:
            flag = "Day is missing"
    else:
        flag = "Month is missing"
    return(flag_data, flag, img_list)

path = 'working path'
df_mic = pd.read_csv(path+'...data path', parse_dates=True, index_col="datetime")
path_ftp = '/cams/All-Sky/lapup/'
nearest_imgs = []
for idx in tqdm.tqdm(df_mic.index):    
    ftp = ftplib.FTP("IP","Username", "Password")
    date_mic = idx.strftime("%Y/%m/%d %H:%M:%S") 
    flag_data, flag_info, img_list = Finding_the_path (date_mic)       
    if (flag_data == True):  
        img_list_images = []
        for img_date in img_list[2:]:           
            if img_date.startswith('Cam'):
                date_img = re.split('[_ .]', img_date)
                img_list_images.append(datetime.strptime(date_img[2], '%Y%m%d%H%M%S'))
            else:
                date_img = re.split('[_ .]', img_date)
                img_list_images.append(datetime.strptime(date_img[0]+date_img[1], '%Y%m%d%H%M%S'))                
        if len(img_list_images) != 0: # if folder is not empty
            test_date = datetime.strptime(date_mic, "%Y/%m/%d %H:%M:%S")           
            cloz_dict = {abs(test_date.timestamp() - date.timestamp()) : date
              for date in img_list_images}
             
            res = cloz_dict[min(cloz_dict.keys())]
            min_dif = np.abs((res - test_date).total_seconds())/60           
            if min_dif < 3:
                closest_datetime = res.strftime("%Y%m%d%H%M%S")               
                if img_date.startswith('Cam'):
                    donwload_image ="Cam_Akaza_" + closest_datetime + ".jpg"              
                elif 'L' in img_date: 
                    date_L = closest_datetime[:8]+"_" + closest_datetime[8:]
                    donwload_image = [f for f in img_list if f.startswith(date_L)][0]             
                else:
                    donwload_image = closest_datetime[:8]+"_" + closest_datetime[8:] + ".jpg"              
                nearest_imgs.append({"datetime":date_mic, "datetime_img":res.strftime("%Y/%m/%d %H:%M:%S"), "image":donwload_image,"Flagging": flag_info})           
                ftp.retrbinary("RETR " + donwload_image , open(path+"/ASI_images/" + donwload_image, 'wb').write)
            else:
                nearest_imgs.append({"datetime":date_mic, "datetime_img":res.strftime("%Y/%m/%d %H:%M:%S"), "Flagging": "Minute difference > 3"})            
    else:
        nearest_imgs.append({"datetime":date_mic, "datetime_img":res.strftime("%Y/%m/%d %H:%M:%S"), "Flagging":flag_info})           

df_imgs = pd.DataFrame(nearest_imgs)
df_imgs.index = pd.to_datetime(df_imgs['datetime'])
df_imgs.index = df_imgs.index.tz_localize('Etc/GMT+0')
df_merged = pd.concat([df_mic, df_imgs],axis=1)
df_merged.to_csv(path+'/ASI_mic_cor_data/ASI_mic_data.csv')

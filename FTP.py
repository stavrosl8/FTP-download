from datetime import datetime
import pandas as pd
import numpy as np
import ftplib
import tqdm
import re

path = 'C:/Users/Stavros/OneDrive - University of Patras/Mic_Cam_data_ftp'

df_mic = pd.read_csv(path+'/Mic_cor_data/Patras_2021-04-12_2023-05-30_filtered.csv', parse_dates=True, index_col="datetime")

path_ftp = '/cams/All-Sky/lapup/'
nearest_imgs = []
for idx in tqdm.tqdm(df_mic.index):
    ftp = ftplib.FTP("150.140.194.29","researcher", "4vkGqU5FNYmZBE8")

    date_mic = idx.strftime("%Y/%m/%d %H:%M:%S")
    dates = date_mic.split(' ')
    path_images_ym = dates[0][:-3]
    path_images_ymd = dates[0]
    path_images_ymdh = path_images_ymd + "/" + dates[1].split(':')[0]
    flag_m, flag_d = False, False    
    try:
        path_images = path_ftp + path_images_ym
        ftp.cwd(path_images)  
        flag_m = True
    except ftplib.error_perm:
        print (date_mic," The month is missing") 
        pass 
   
    if flag_m == True:
        try:
            path_images = path_ftp + path_images_ymd
            ftp.cwd(path_images)
            flag_d = True
        except ftplib.error_perm:
            print (date_mic," The day is missing") 
            pass
        
    if flag_d == True:
        try:
            path_images = path_ftp + path_images_ymdh
            ftp.cwd(path_images)
            
        except ftplib.error_perm:
            path_images = path_ftp + path_images_ymd
            ftp.cwd(path_images)
  
    if (flag_d == True) & (flag_m == True):    
        img_list = ftp.nlst()
        img_list_images = []
        for img_date in img_list[2:]:
            
            if img_date.startswith('Cam'):
                date_img = re.split('[_ .]', img_date)
                img_list_images.append(datetime.strptime(date_img[2], '%Y%m%d%H%M%S'))
            else:
                date_img = re.split('[_]', img_date)
                img_list_images.append(datetime.strptime(date_img[0]+date_img[1], '%Y%m%d%H%M%S'))
                
        if len(img_list_images) != 0: # if folder is not empty

            test_date = datetime.strptime(date_mic, "%Y/%m/%d %H:%M:%S")
             
            cloz_dict = {abs(test_date.timestamp() - date.timestamp()) : date
              for date in img_list_images}
             
            res = cloz_dict[min(cloz_dict.keys())]
            min_dif = np.abs((res - test_date).total_seconds())/60
           
            if min_dif < 3:
                closest_datetime = res.strftime("%Y%m%d%H%M%S")
                donwload_image ="Cam_Akaza_" + closest_datetime + ".jpg"              
                nearest_imgs.append({"datetime":date_mic, "datetime_img":res.strftime("%Y/%m/%d %H:%M:%S"), "image":donwload_image})
            
                ftp.retrbinary("RETR " + donwload_image , open(path+"/ASI_images/" + donwload_image, 'wb').write)

df_imgs = pd.DataFrame(nearest_imgs)

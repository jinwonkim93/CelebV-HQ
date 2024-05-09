import fire
from pathlib import Path
import subprocess
import json
import os
import ray

@ray.remote
def get_video_info(video_id, json_path, id_index):
    command = ["yt-dlp", "--dump-json", f"https://www.youtube.com/watch?v={video_id}"]
    video_metadata_path = Path(os.path.join(json_path, id_index))
    video_metadata_path.mkdir(exist_ok=True, parents=True)
    
    with open(video_metadata_path/f"{video_id}.json", 'w') as file:

        result = subprocess.run(command, stdout=file, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        print("오류 발생:")
        print(result.stderr)
    
    
def get_vox_metadata(vox_path, json_path):
    
    ray.init()
    
    ids_path = sorted(list(Path(vox_path).glob("*")))
    results = []
    for i, id_path in enumerate(ids_path):
        
        videos_path = sorted(list(Path(id_path).glob("*")))
#         print('Identity {}/{}: {} videos for {} identity'.format(i, len(ids_path), len(videos_path), id_path.name))
        
        for j, video_path in enumerate(videos_path):
#             print('{}/{} videos'.format(j, len(videos_path)))
            
            results.append(get_video_info.remote(video_path.name, json_path, id_path.name))
    ray.get(results)
            
if __name__ == "__main__":
    fire.Fire(get_vox_metadata)
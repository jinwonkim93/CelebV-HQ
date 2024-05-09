import fire
from pathlib import Path
import subprocess
import json
import os
import ray

@ray.remote
def download(video_id, raw_vid_root, id_index):
    
    Path(os.path.join(raw_vid_root, id_index)).mkdir(exist_ok=True, parents=True)
    video_path = os.path.join(raw_vid_root, id_index, video_id + ".mp4")
    command = [
            "yt-dlp",
            '--skip-unavailable-fragments',
            '--merge-output-format', 'mp4',
            "-S", "ext:mp4:m4a",
            "https://www.youtube.com/watch?v=" + video_id, "--output",
            video_path,
        ]

    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

    if result.returncode != 0:
        print("오류 발생:")
        print(result.stderr)


def download_vox(json_path, video_path, parallel=4):
    
    ray.init()
    
    ids_path = sorted(list(Path(json_path).glob("*")))
    results = []
    for i, id_path in enumerate(ids_path):
        
        videos_meta_path = sorted(list(Path(id_path).glob("*.json")))
        
        for j, meta_path in enumerate(videos_meta_path):
            try:
                with open(str(meta_path), 'r') as file:
                    meta = json.load(file)
                    if min(meta['width'], meta['height']) < 720:
                        continue
            except:
                continue
            
            if len(results) > parallel:
                ready_refs, results = ray.wait(results, num_returns=1)
                ray.get(ready_refs)
                
            results.append(download.remote(meta_path.stem, video_path, id_path.name))
           
    ray.get(results)

if __name__ == "__main__":
    fire.Fire(download_vox)

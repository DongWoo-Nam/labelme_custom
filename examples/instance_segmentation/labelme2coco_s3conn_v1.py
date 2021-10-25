#!/usr/bin/env python
import argparse
import collections
import datetime
import json
import os
import os.path as osp
import sys
import uuid
from ObjectStorageHandler import *
import numpy as np
import labelme

# ex) python labelme2coco_s3conn_v1.py process03 test --labels=shrimp_labels.txt --prefix=shrimp
#                                      bucket   out_dir label file                   bucket directory

def label_check(labels, bucket_directory):
    if bucket_directory == "shrimp":
        if labels in ['type 01/wt11']:  # 라벨에 오타를 입력한 작업자가 있음
            label = "shrimp_type01_bb"
        elif labels in [':shrimp_type02_bb', 'shirmp/type02/wt14_bb', 'type 02/wt11']:
            label = "shrimp_type02_bb"
        elif labels in ['shirmp_ty03_wt23_poly', 'shrimp_ty03_poly', 'type 03/wt11', 'type 03/wt22',
                        'shirmp_ty03_poly', 'shrimp_type03_bb']:
            label = "shrimp_type03_poly"
        else:
            label = labels
        return label

    elif bucket_directory == "tomato":
        import re
        if u'\u200b' in labels:  # 라벨에 유니코드 문자인 너비없는공백을 입력 한 작업자가 있음
            w = labels.strip()
            w = re.sub(r"[^0-9a-zA-Z?.!,¿_]+", " ", w)  # \n도 공백으로 대체해줌
            label = w.strip()
        elif labels in ['tom_leaf_004', 'tom_leaf_002', 'tom_leaf_005', 'tom_leaf_001', 'tom_leaf_003', 'tom_leaf_006']:
            label = "tom_leaf_bb"
        elif labels in ['tom_stem_diameter_line']:
            label = "tom_stem_dimeter_line"
        elif labels in ['tomato_fruit_red']:
            label = "tom_fruit_red_poly"
        else:
            label = labels
        return label

    elif bucket_directory == "paprika":
        pass

    elif bucket_directory == "":
        pass


try:
    import pycocotools.mask
except ImportError:
    print("Please install pycocotools:\n\n    pip install pycocotools\n")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("bucket", help="input annotated directory")
    parser.add_argument("output_dir", help="output dataset directory")
    parser.add_argument("--labels", help="labels file", required=True)
    parser.add_argument("--prefix", help="bucket prefix",default='')

    args = parser.parse_args()

    if osp.exists(args.output_dir):
        print("Output directory already exists:", args.output_dir)
        sys.exit(1)
    os.makedirs(args.output_dir)

    print("Creating dataset:", args.output_dir)

    now = datetime.datetime.now()

    data = dict(
        info=dict(
            description=None,
            url=None,
            version=None,
            year=now.year,
            contributor=None,
            date_created=now.strftime("%Y-%m-%d %H:%M:%S.%f"),
        ),
        licenses=[dict(url=None, id=0, name=None,)],
        images=[
            # license, url, file_name, height, width, date_captured, id
        ],
        type="instances",
        annotations=[
            # segmentation, area, iscrowd, image_id, bbox, category_id, id
        ],
        categories=[
            # supercategory, id, name
        ],
    )

    class_name_to_id = {}
    for i, line in enumerate(open(args.labels).readlines()):
        class_id = i - 1  # starts with -1
        class_name = line.strip()
        if class_id == -1:
            assert class_name == "__ignore__"
            continue
        class_name_to_id[class_name] = class_id
        data["categories"].append(
            dict(supercategory=None, id=class_id, name=class_name,)
        )

    out_ann_file = osp.join(args.output_dir, "annotations.json") # coco.json
    conn_s3 = s3_loader(args.bucket)
    prefix = ''
    if args.prefix != '':
        prefix = args.prefix
    label_files = conn_s3.s3_list(prefix)

    import tqdm
    image_id = 0
    for label_files_check in tqdm.tqdm(label_files, desc="error file check"):
        json_io = conn_s3.read_files(label_files_check)
        json_data = json.loads(json_io.getvalue().decode())

        x = 0
        for shape in json_data["shapes"]:
            points = shape["points"]
            shape_type = shape["shape_type"]
            if (len(points) < 3) & (shape_type == "polygon"):
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\tpolygon인데 points가 3개미만으로 있음")
                    f.write("\n")
            elif shape_type == "line":
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\tshape type이 line")
                    f.write("\n")
            elif len(points) < 2:
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\t점이 2개 미만")
                    f.write("\n")
        if len(json_data["shapes"]) == 0:
            with open("error.txt", "a+") as f:
                f.write(label_files_check)
                f.write("\tjson내에 내용 없음")
                f.write("\n")
        elif x == "error":
            pass
        else:
            filename = label_files_check
            image_id += 1

            print("Generating dataset from:", filename)

            data["images"].append(
                dict(
                    license=0,
                    url=None,
                    # file_name=osp.relpath(out_img_file, osp.dirname(out_ann_file)),
                    file_name=label_files_check.replace('json',osp.basename(json_data['imagePath']).split('.')[-1]),
                    height=json_data['imageHeight'],
                    width=json_data['imageWidth'],
                    date_captured=None,
                    id=image_id,
                )
            )

            masks = {}  # for area
            segmentations = collections.defaultdict(list)  # for segmentation
            for shape in json_data["shapes"]:
                img_shape = (json_data['imageHeight'], json_data['imageWidth'])
                points = shape["points"]
                labels = shape["label"]
                label = label_check(labels=labels, bucket_directory=args.prefix)
                group_id = shape["group_id"]
                shape_type = shape["shape_type"]
                mask = labelme.utils.shape_to_mask(
                    img_shape, points, shape_type
                )

                if group_id is None:
                    group_id = uuid.uuid1()

                instance = (label, group_id)

                if instance in masks:
                    masks[instance] = masks[instance] | mask
                else:
                    masks[instance] = mask

                if shape_type == "rectangle":
                    (x1, y1), (x2, y2) = points
                    x1, x2 = sorted([x1, x2])
                    y1, y2 = sorted([y1, y2])
                    points = [x1, y1, x2, y1, x2, y2, x1, y2]
                else:
                    points = np.asarray(points).flatten().tolist()

                segmentations[instance].append(points)
            segmentations = dict(segmentations)

            for instance, mask in masks.items():
                cls_name, group_id = instance
                if cls_name not in class_name_to_id:
                    continue
                cls_id = class_name_to_id[cls_name]

                mask = np.asfortranarray(mask.astype(np.uint8))
                mask = pycocotools.mask.encode(mask)
                area = float(pycocotools.mask.area(mask))
                bbox = pycocotools.mask.toBbox(mask).flatten().tolist()

                data["annotations"].append(
                    dict(
                        id=len(data["annotations"]),
                        image_id=image_id,
                        category_id=cls_id,
                        segmentation=segmentations[instance],
                        area=area,
                        bbox=bbox,
                        iscrowd=0,
                    )
                )

        with open(out_ann_file, "w") as f:
            json.dump(data, f, indent=4)


if __name__ == "__main__":
    main()

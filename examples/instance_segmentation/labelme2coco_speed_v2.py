#!/usr/bin/env python

import argparse
import collections
import datetime
import glob
import json
import os
import os.path as osp
import sys
import uuid

import imgviz
from PIL import Image
import numpy as np

import labelme

try:
    import pycocotools.mask
except ImportError:
    print("Please install pycocotools:\n\n    pip install pycocotools\n")
    sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument("input_dir", help="input annotated directory")
    parser.add_argument("output_dir", help="output dataset directory")
    parser.add_argument("--labels", help="labels file", required=True)
    parser.add_argument(
        "--noviz", help="no visualization", action="store_true"
    )
    args = parser.parse_args()

    if osp.exists(args.output_dir):
        print("Output directory already exists:", args.output_dir)
        sys.exit(1)
    os.makedirs(args.output_dir)
    os.makedirs(osp.join(args.output_dir, "JPEGImages"))
    if not args.noviz:
        os.makedirs(osp.join(args.output_dir, "Visualization"))
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

    out_ann_file = osp.join(args.output_dir, "annotations.json")
    label_files = glob.glob(osp.join(args.input_dir, "*.json"))

    label_files_chck = []  # json?????? shapes????????? ???????????? ?????? ?????? ??? ??????
    import tqdm
    for label_files_check in tqdm.tqdm(label_files, desc="error file check"):
        # label_file = labelme.LabelFile(filename=label_files_check)
        with open(label_files_check, 'r') as f:
            json_data = json.load(f)
        x = 0
        for shape in json_data["shapes"]:
            points = shape["points"]
            shape_type = shape["shape_type"]
            if (len(points) < 3) & (shape_type == "polygon"):
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\tpolygon?????? points??? 3??????????????? ??????")
                    f.write("\n")
            elif shape_type == "line":
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\tshape type??? line")
                    f.write("\n")
            elif len(points) < 2:
                x = "error"
                with open("error.txt", "a+") as f:
                    f.write(label_files_check)
                    f.write("\t?????? 2??? ??????")
                    f.write("\n")
        if len(json_data["shapes"]) == 0:
            with open("error.txt", "a+") as f:
                f.write(label_files_check)
                f.write("\tjson?????? ?????? ??????")
                f.write("\n")
        elif x == "error":
            pass
        else:
            label_files_chck.append(label_files_check)

    for image_id, filename in enumerate(label_files_chck):
        print("Generating dataset from:", filename)

        # label_file = labelme.LabelFile(filename=filename)
        json_data = json.load(open(filename))  ###############
        img_name = json_data['imagePath']  ###############

        # base = osp.splitext(osp.basename(filename))[0]
        # out_img_file = osp.join(args.output_dir, "JPEGImages", base + ".jpg")
        out_img_file = osp.join(args.output_dir, "JPEGImages", img_name)  # png, jpg ???????????? ?????? ????????? ???????????? ???????????? ??????

        # json_path = os.path.join(self._json_dir, json_name)
        # json_data = json.load(open(json_path))


        import shutil
        src = os.path.join(args.input_dir, img_name)
        dst = out_img_file
        shutil.copy(src=src, dst=dst)
        # origin_img = Image.open(os.path.join(args.input_dir, img_name))          ############### pillow??? ?????? ?????? ????????? ???????????? ???????????? ?????????!
        # origin_img.save(out_img_file)          ###############

        # img = labelme.utils.img_data_to_arr(label_file.imageData)
        # imgviz.io.imsave(out_img_file, img)

        data["images"].append(
            dict(
                license=0,
                url=None,
                file_name=osp.relpath(out_img_file, osp.dirname(out_ann_file)),
                height=json_data['imageHeight'],
                width=json_data['imageWidth'],
                # height=img.shape[0],
                # width=img.shape[1],
                date_captured=None,
                id=image_id,
            )
        )

        masks = {}  # for area
        segmentations = collections.defaultdict(list)  # for segmentation
        # with open(filename, 'r') as f:          ###############
        #     json_data = json.load(f)            ###############
        for shape in json_data["shapes"]:       ###############
        # for shape in label_file.shapes:
            img_shape = (json_data['imageHeight'], json_data['imageWidth'])
            points = shape["points"]
            label = shape["label"]
            # group_id = shape.get("group_id")
            # shape_type = shape.get("shape_type", "polygon")
            group_id = shape["group_id"]        ###############
            shape_type = shape["shape_type"]    ###############
            mask = labelme.utils.shape_to_mask( ###############
                img_shape, points, shape_type
            )
            # mask = labelme.utils.shape_to_mask(
            #     img.shape[:2], points, shape_type
            # )

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

        if not args.noviz:
            label_file = labelme.LabelFile(filename=filename)  ###############
            base = osp.splitext(osp.basename(filename))[0]
            img = labelme.utils.img_data_to_arr(label_file.imageData)  ###############
            labels, captions, masks = zip(
                *[
                    (class_name_to_id[cnm], cnm, msk)
                    for (cnm, gid), msk in masks.items()
                    if cnm in class_name_to_id
                ]
            )
            viz = imgviz.instances2rgb(
                image=img,
                labels=labels,
                masks=masks,
                captions=captions,
                font_size=15,
                line_width=2,
            )
            out_viz_file = osp.join(
                args.output_dir, "Visualization", base + ".jpg"
            )
            imgviz.io.imsave(out_viz_file, viz)
    with open(out_ann_file, "w") as f:
        json.dump(data, f, indent=4)


if __name__ == "__main__":
    main()

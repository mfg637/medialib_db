import time

import PIL.Image
import torch
import open_clip
import multiprocessing

try:
    from . import config
except ImportError:
    import config

try:
    from . import common
except ImportError:
    import common


def make_clip_classification(
        image: PIL.Image.Image, shared_state_list_proxy, results_pipe: multiprocessing.connection.Connection,
        tag_len_min=4,
        tag_len_max=8,
        max_tags_per_iteration=25,
        max_tags_count=100,
        max_tag_probability=0.5
):
    connection = common.make_connection()
    cursor = connection.cursor()

    sql_get_tag_names = (
        "SELECT id, title FROM tag where length(title) <= %s and length(title) >= %s "
        "and category != 'artist' and category != 'set';"
    )
    cursor.execute(sql_get_tag_names, (tag_len_max, tag_len_min))

    model, _, preprocess = open_clip.create_model_and_transforms('ViT-B-32-quickgelu', pretrained='laion400m_e32')
    tokenizer = open_clip.get_tokenizer('ViT-B-32-quickgelu')

    _image = preprocess(image).unsqueeze(0)
    labels = []
    ids = []
    image_features = model.encode_image(_image)
    image_features /= image_features.norm(dim=-1, keepdim=True)

    tag_name = cursor.fetchone()
    while tag_name is not None:
        labels.append(tag_name[1])
        ids.append(tag_name[0])
        tag_name = cursor.fetchone()
    cursor.close()
    connection.close()

    current_iteration = 0
    MAX_ITERATIONS = len(labels)
    shared_state_list_proxy.total = MAX_ITERATIONS
    results_list = []
    while current_iteration < MAX_ITERATIONS:
        shared_state_list_proxy.done = current_iteration
        _labels = labels[current_iteration: current_iteration + max_tags_per_iteration]
        _ids = ids[current_iteration: current_iteration + max_tags_per_iteration]

        text = tokenizer(_labels)

        with torch.no_grad(), torch.cuda.amp.autocast():
            text_features = model.encode_text(text)
            text_features /= text_features.norm(dim=-1, keepdim=True)
            text_probs = (100.0 * image_features @ text_features.T).softmax(dim=-1)

        results = zip(_ids, _labels, text_probs.tolist()[0])

        results_list.extend(results)

        current_iteration += max_tags_per_iteration

    def sort_results(e):
        return e[2]

    results_list.sort(key=sort_results, reverse=True)

    if max_tags_count is not None:
        filtered_results_list = []
        for i in range(max_tags_count):
            if results_list[i][2] >= max_tag_probability:
                filtered_results_list.append(results_list[i])
            else:
                break
        results_pipe.send(filtered_results_list)
    else:
        results_pipe.send(results_list)


def start_clip_classification_process(img: PIL.Image.Image):
    manager = multiprocessing.Manager()
    shared_state = manager.Namespace()
    shared_state.done = 0
    shared_state.total = 0
    result_pipe = multiprocessing.Pipe(duplex=False)
    process = multiprocessing.Process(target=make_clip_classification, args=(img, shared_state, result_pipe[1]))
    process.start()
    return process, manager, shared_state, result_pipe[0]


def get_clip_classified_result(img: PIL.Image.Image):
    process, manager, shared_state, result_pipe = start_clip_classification_process(img)
    time.sleep(1)
    while process.is_alive():
        print(f"{shared_state.done}/{shared_state.total}")
        time.sleep(1)
    return result_pipe.recv()


if __name__ == '__main__':
    import pathlib
    import argparse
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('imagefile', type=pathlib.Path)
    args = argument_parser.parse_args()
    img = PIL.Image.open(args.imagefile)

    results = get_clip_classified_result(img)
    for result in results:
        print("{}: {}".format(result[1], result[2]))

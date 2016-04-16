for i in range(1,17771):
    nome = "mv_00"+str(i).zfill(5)+".txt"
    with open("/Users/Rocha/Documents/Datasets/download/training_set/"+nome, 'r+') as movie_file:
        linhas = movie_file.readlines()
        movie_file.seek(0)
        for line in linhas:
            if ':' not in line:
                movie_file.write(str(i)+","+line)

with open("data/probe.txt", 'r+') as probe_file:
    linhas = probe_file.readlines()
    probe_file.seek(0)
    current_movie_id = ""
    for line in linhas:
        if ':' in line:
            current_movie_id = line[:-2]
        if ':' not in line:
            probe_file.write(current_movie_id+","+line)

with open("data/qualifying.txt", 'r+') as qualifying_file:
    linhas = qualifying_file.readlines()
    qualifying_file.seek(0)
    current_movie_id = ""
    for line in linhas:
        if ':' in line:
            current_movie_id = line[:-2]
        if ':' not in line:
            qualifying_file.write(current_movie_id+","+line)

with open("data/probe.txt", 'r+') as probe_file:
    linhas = probe_file.readlines()
    probe_file.seek(0)
    for line in linhas:
        line_parts = line.split(',')
        movie_number = line_parts[0]
        user_number = line_parts[1][:-1]
        movie_file_name = "mv_00"+str(movie_number).zfill(5)+".txt"
        with open("/Users/Rocha/Documents/Datasets/download/training_set/"+movie_file_name, 'r+') as movie_file:
            movie_lines = movie_file.readlines()
            for movie_line in movie_lines:
                parts = movie_line.split(',')
                if parts[1] == user_number:
                    new_line = line[:-1] + ',' + parts[2]+'\n'
                    probe_file.write(new_line)
                    break

with open("data/probe.txt", 'r+') as probe_file:
    linhas = probe_file.readlines()
    dic = {}
    movie_id = ''
    for line in linhas:
        line_parts = line.split(',')
        if line_parts[0] != movie_id:
            dic[line_parts[0]] = [line_parts[1]]
            movie_id = line_parts[0]
        else:
            dic[line_parts[0]].append(line_parts[1])

for i in range(1,17770):
    if str(i) in dic.keys():
        nome = "mv_00"+str(i).zfill(5)+".txt"
        print("Ficheiro: "+nome)
        with open("/Users/Rocha/Documents/Datasets/download/training_set_original/"+nome, 'r+') as movie_file:
            linhas = movie_file.readlines()
            with open("/Users/Rocha/Documents/Datasets/download/training_set/"+nome, 'w+') as movie_file_novo:
                for line in linhas:
                    line_parts = line.split(',')
                    if line_parts[1] not in dic[line_parts[0]]:
                        movie_file_novo.write(line)



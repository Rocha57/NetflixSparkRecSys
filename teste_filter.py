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
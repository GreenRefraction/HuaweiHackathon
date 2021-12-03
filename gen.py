import Debug
import random

if __name__ == "__main__":
    for epoc in range(100):
        bace = Debug.COFS
        if epoc % 5 == 4:
            c0 = random.random()
            c1 = random.random()
            c2 = random.random()
            c3 = random.random()
            bace = (bace[0] + c0,
                    bace[1] + c1,
                    bace[2] + c2,
                    bace[3] + c3)
            print("Random move")

        cofs = []
        res = []
        prog = []
        for i in range(100):
            c0 = random.random()
            c1 = random.random()
            c2 = random.random()
            c3 = random.random()
            cofs.append((c0, c1, c2, c3))

            Debug.COFS = (bace[0] + c0,
                          bace[1] + c1,
                          bace[2] + c2,
                          bace[3] + c3)
            processor_list, dag_list, env = Debug.main(
                "testcases/test12.json", "answer12.csv", 6)
            res.append(env.time_stamp)
            prog.append(env.time_stamp / env.last_deadline)
            processor_list, dag_list, env = Debug.main(
                "testcases/test12.json", "answer12.csv", 6)
            res[-1] += (env.time_stamp)
            prog[-1] += (env.time_stamp / env.last_deadline)

            print(f"\r{i}", end="")
        print()

        mi = res.index(max(res))

        Debug.COFS = (bace[0] + cofs[mi][0],
                      bace[1] + cofs[mi][1],
                      bace[2] + cofs[mi][2],
                      bace[3] + cofs[mi][3])

        # Norm
        min_c = min(Debug.COFS)
        Debug.COFS = (Debug.COFS[0]/min_c,
                      Debug.COFS[1]/min_c, 
                      Debug.COFS[2]/min_c,
                      Debug.COFS[3]/min_c)

        print("===", res[mi], ">", Debug.COFS)
        print(f"{prog[mi]}% completed")

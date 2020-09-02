using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace FASTER.serverless
{
    public interface IDprTableSnapshot
    {
        long SafeVersion(Worker worker);
    }
    
    public interface IDprManager
    {
        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        long SafeVersion(Worker worker);
        
        IDprTableSnapshot ReadSnapshot();

        long SystemWorldLine();

        long GlobalMaxVersion();

        /// <summary>
        /// 
        /// </summary>
        /// <param name="newVersion"></param>
        /// <returns></returns>
        void ReportNewPersistentVersion(WorkerVersion persisted, IEnumerable<WorkerVersion> deps);
        
        /// <summary>
        /// 
        /// </summary>
        /// <returns>system world line</returns>
        void Refresh();

        void Clear();

        void ReportRecovery(long worldLine, WorkerVersion latestRecoveredVersion);
    }
}

/*
17.785029190992493, p99 latency 45, number of workers: 4, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.651584653878232, p99 latency 29, number of workers: 8, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.157554257095159, p99 latency 29, number of workers: 12, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.459414981154112, p99 latency 106, number of workers: 16, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.005462885738115, p99 latency 29, number of workers: 20, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.79260494856825, p99 latency 30, number of workers: 24, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
13.66400548124758, p99 latency 30, number of workers: 28, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.513711485324018, p99 latency 22, number of workers: 32, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.922687475324555, p99 latency 30, number of workers: 36, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.116842260836519, p99 latency 24, number of workers: 40, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
20.343343748794506, p99 latency 118, number of workers: 44, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.799867945510147, p99 latency 36, number of workers: 48, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.891871029991155, p99 latency 45, number of workers: 52, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
17.368406905202797, p99 latency 44, number of workers: 56, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.935560825697085, p99 latency 43, number of workers: 60, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.44137713865855, p99 latency 43, number of workers: 64, dprType: v1, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.119674185463658, p99 latency 35, number of workers: 4, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.3222813053904705, p99 latency 30, number of workers: 8, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
17.58312182741117, p99 latency 105, number of workers: 12, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.188907422852377, p99 latency 30, number of workers: 16, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.227522935779817, p99 latency 32, number of workers: 20, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.522414512093412, p99 latency 32, number of workers: 24, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.426868873461848, p99 latency 30, number of workers: 28, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.783048373644704, p99 latency 22, number of workers: 32, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.604079740380158, p99 latency 32, number of workers: 36, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.115606936416185, p99 latency 31, number of workers: 40, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
25.0343964128346, p99 latency 139, number of workers: 44, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.789487402258906, p99 latency 35, number of workers: 48, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.48035720021843, p99 latency 37, number of workers: 52, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
18.10526237395177, p99 latency 39, number of workers: 56, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.156266863586016, p99 latency 46, number of workers: 60, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
25.292038166744877, p99 latency 45, number of workers: 64, dprType: v1, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.508340283569641, p99 latency 34, number of workers: 4, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
9.283554072374596, p99 latency 30, number of workers: 8, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
25.045112781954888, p99 latency 115, number of workers: 12, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.674555596100713, p99 latency 32, number of workers: 16, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.615095913261051, p99 latency 31, number of workers: 20, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.179009556907037, p99 latency 30, number of workers: 24, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.244727749314904, p99 latency 23, number of workers: 28, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.920333680917622, p99 latency 22, number of workers: 32, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.457069361080855, p99 latency 23, number of workers: 36, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.437686357096688, p99 latency 29, number of workers: 40, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
23.996054103928227, p99 latency 143, number of workers: 44, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.955620427809345, p99 latency 35, number of workers: 48, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.031873916672016, p99 latency 39, number of workers: 52, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.77743479622422, p99 latency 38, number of workers: 56, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.855674151787593, p99 latency 40, number of workers: 60, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.93574978152839, p99 latency 41, number of workers: 64, dprType: v1, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.631359466221852, p99 latency 34, number of workers: 4, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.813210894291975, p99 latency 32, number of workers: 8, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
23.10482541048254, p99 latency 108, number of workers: 12, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.626511676396998, p99 latency 29, number of workers: 16, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.440116763969975, p99 latency 29, number of workers: 20, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.660341951626355, p99 latency 31, number of workers: 24, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.376742523531515, p99 latency 31, number of workers: 28, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.494292118432027, p99 latency 25, number of workers: 32, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.085414782346529, p99 latency 25, number of workers: 36, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.696673965175686, p99 latency 34, number of workers: 40, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
26.726427028394607, p99 latency 143, number of workers: 44, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.805901160770139, p99 latency 36, number of workers: 48, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.014823261117446, p99 latency 39, number of workers: 52, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.861889241029186, p99 latency 37, number of workers: 56, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.70331855023704, p99 latency 43, number of workers: 60, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.39865164436794, p99 latency 43, number of workers: 64, dprType: v1, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.818807339449541, p99 latency 30, number of workers: 4, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.802461410095953, p99 latency 31, number of workers: 8, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
25.804378731873754, p99 latency 114, number of workers: 12, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.72398874061718, p99 latency 31, number of workers: 16, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.19020016680567, p99 latency 30, number of workers: 20, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
10.943181818181818, p99 latency 24, number of workers: 24, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
9.842351468923187, p99 latency 20, number of workers: 28, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.76470741561319, p99 latency 32, number of workers: 32, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.846538685282141, p99 latency 33, number of workers: 36, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.736500584209647, p99 latency 32, number of workers: 40, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
20.656090662459356, p99 latency 107, number of workers: 44, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.065590295277985, p99 latency 35, number of workers: 48, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.586294375491724, p99 latency 39, number of workers: 52, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.823225402877483, p99 latency 40, number of workers: 56, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.398789649415694, p99 latency 42, number of workers: 60, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
20.451609934393037, p99 latency 41, number of workers: 64, dprType: v1, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.757247132429614, p99 latency 32, number of workers: 4, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
8.380857948022127, p99 latency 31, number of workers: 8, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.3390937829294, p99 latency 98, number of workers: 12, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
9.958298582151793, p99 latency 30, number of workers: 16, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.291762252346194, p99 latency 31, number of workers: 20, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.885033989644604, p99 latency 33, number of workers: 24, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.695036939942803, p99 latency 32, number of workers: 28, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.272732020682092, p99 latency 31, number of workers: 32, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.983519540123314, p99 latency 33, number of workers: 36, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
20.04248643147897, p99 latency 121, number of workers: 40, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.85212737693096, p99 latency 33, number of workers: 44, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.58236562130589, p99 latency 36, number of workers: 48, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.831758003690926, p99 latency 38, number of workers: 52, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.770886472457153, p99 latency 41, number of workers: 56, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.42021224807711, p99 latency 43, number of workers: 60, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.4486983554737, p99 latency 45, number of workers: 64, dprType: v1, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.418769551616267, p99 latency 43, number of workers: 4, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.183858392160994, p99 latency 70, number of workers: 8, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.837538332868693, p99 latency 76, number of workers: 12, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.72303602147735, p99 latency 30, number of workers: 16, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.376344982900992, p99 latency 31, number of workers: 20, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.915116966178873, p99 latency 31, number of workers: 24, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.52479203315543, p99 latency 31, number of workers: 28, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.39775837351753, p99 latency 30, number of workers: 32, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.179199332003526, p99 latency 26, number of workers: 36, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.536507665006095, p99 latency 141, number of workers: 40, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.177541729893779, p99 latency 33, number of workers: 44, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.602687340297937, p99 latency 35, number of workers: 48, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.10580626524586, p99 latency 37, number of workers: 52, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.941184407796102, p99 latency 38, number of workers: 56, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.546608577036036, p99 latency 42, number of workers: 60, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.338032945104533, p99 latency 44, number of workers: 64, dprType: v1, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
8.228875443354893, p99 latency 28, number of workers: 4, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
4.897438600189734, p99 latency 23, number of workers: 8, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
5.285168195718654, p99 latency 23, number of workers: 12, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
5.961422166614534, p99 latency 18, number of workers: 16, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.414804003336114, p99 latency 21, number of workers: 20, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.079093689185433, p99 latency 17, number of workers: 24, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.0609173435260395, p99 latency 16, number of workers: 28, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.421601334445371, p99 latency 17, number of workers: 32, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.727118958246443, p99 latency 17, number of workers: 36, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.707864926562433, p99 latency 30, number of workers: 40, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.8456857987717035, p99 latency 17, number of workers: 44, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
8.16133077861, p99 latency 20, number of workers: 48, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
8.10255340989286, p99 latency 20, number of workers: 52, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.819357144984957, p99 latency 19, number of workers: 56, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
8.759265741609376, p99 latency 19, number of workers: 60, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
8.417647672992091, p99 latency 18, number of workers: 64, dprType: v2, depProb: 0, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
7.026897414512093, p99 latency 30, number of workers: 4, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.52759826252781, p99 latency 93, number of workers: 8, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
9.575688073394495, p99 latency 30, number of workers: 12, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
6.718723936613845, p99 latency 19, number of workers: 16, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.624061718098416, p99 latency 29, number of workers: 20, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.48047974969581, p99 latency 33, number of workers: 24, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
13.170382973642578, p99 latency 31, number of workers: 28, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.927802538639977, p99 latency 31, number of workers: 32, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.6702036466418, p99 latency 31, number of workers: 36, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.35371689655919, p99 latency 137, number of workers: 40, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
17.31748984472875, p99 latency 36, number of workers: 44, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.41118552665913, p99 latency 39, number of workers: 48, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.678694408070225, p99 latency 47, number of workers: 52, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
22.988698609522665, p99 latency 44, number of workers: 56, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
24.187439949871198, p99 latency 46, number of workers: 60, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
23.714272551368285, p99 latency 47, number of workers: 64, dprType: v2, depProb: 0.25, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.262927439532945, p99 latency 42, number of workers: 4, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.408807688245854, p99 latency 80, number of workers: 8, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
13.809285515707534, p99 latency 31, number of workers: 12, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
13.87129899916597, p99 latency 32, number of workers: 16, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.137236084452976, p99 latency 29, number of workers: 20, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.646889120611748, p99 latency 29, number of workers: 24, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
13.154151258499343, p99 latency 29, number of workers: 28, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.103955362031654, p99 latency 34, number of workers: 32, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
20.21875588179936, p99 latency 111, number of workers: 36, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.48381704590568, p99 latency 35, number of workers: 40, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
18.408867649011018, p99 latency 39, number of workers: 44, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.989857905945605, p99 latency 62, number of workers: 48, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.90244176706827, p99 latency 41, number of workers: 52, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.855568419311673, p99 latency 41, number of workers: 56, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
24.4571870342253, p99 latency 49, number of workers: 60, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
25.609401552981776, p99 latency 50, number of workers: 64, dprType: v2, depProb: 0.5, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.135357368753953, p99 latency 69, number of workers: 4, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.811374109761207, p99 latency 69, number of workers: 8, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.594384209063108, p99 latency 31, number of workers: 12, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.857537531276064, p99 latency 31, number of workers: 16, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.403311230660162, p99 latency 31, number of workers: 20, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.550252042412655, p99 latency 32, number of workers: 24, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
18.036480364803648, p99 latency 155, number of workers: 28, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.67511472674176, p99 latency 31, number of workers: 32, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
20.361695036305203, p99 latency 122, number of workers: 36, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.627185478823284, p99 latency 35, number of workers: 40, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
17.002143887075967, p99 latency 37, number of workers: 44, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.51322977277176, p99 latency 57, number of workers: 48, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
20.54983524873423, p99 latency 43, number of workers: 52, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
22.701452014520147, p99 latency 43, number of workers: 56, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.559518172956412, p99 latency 45, number of workers: 60, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
25.39445336675937, p99 latency 50, number of workers: 64, dprType: v2, depProb: 0.75, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.392616033755274, p99 latency 73, number of workers: 4, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
11.618705035971223, p99 latency 31, number of workers: 8, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.168334723380594, p99 latency 32, number of workers: 12, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.270256169457923, p99 latency 32, number of workers: 16, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.41605504587156, p99 latency 31, number of workers: 20, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.569660734149053, p99 latency 33, number of workers: 24, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
12.892618849040867, p99 latency 28, number of workers: 28, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.271587562227957, p99 latency 33, number of workers: 32, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
19.734247033260907, p99 latency 133, number of workers: 36, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
15.409930619409847, p99 latency 38, number of workers: 40, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
14.215139215537516, p99 latency 35, number of workers: 44, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
16.774120100808204, p99 latency 37, number of workers: 48, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
20.531668312264287, p99 latency 44, number of workers: 52, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
21.549004708266285, p99 latency 42, number of workers: 56, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
30.58921846261641, p99 latency 223, number of workers: 60, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
27.03496955157778, p99 latency 47, number of workers: 64, dprType: v2, depProb: 1, heavyHitterProb: 0, delayProb: 0, averageMilli: 25										
10.12158441008261, p99 latency 73, number of workers: 4, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.047544573037223, p99 latency 31, number of workers: 8, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.130872949680288, p99 latency 30, number of workers: 12, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.899900943642146, p99 latency 30, number of workers: 16, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.551058583453788, p99 latency 31, number of workers: 20, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.351786210731165, p99 latency 28, number of workers: 24, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.09033412887828, p99 latency 25, number of workers: 28, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.098996481167731, p99 latency 30, number of workers: 32, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.976148577951108, p99 latency 133, number of workers: 36, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.125145954962468, p99 latency 32, number of workers: 40, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.35287862676591, p99 latency 39, number of workers: 44, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.23452004311094, p99 latency 37, number of workers: 48, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.11544429000873, p99 latency 40, number of workers: 52, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.735797572540484, p99 latency 43, number of workers: 56, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.2289206950323, p99 latency 45, number of workers: 60, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
23.92912995235298, p99 latency 46, number of workers: 64, dprType: v2, depProb: 0, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.41472134595163, p99 latency 57, number of workers: 4, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.344806007509387, p99 latency 31, number of workers: 8, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.559424520433695, p99 latency 29, number of workers: 12, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
10.925402700307563, p99 latency 30, number of workers: 16, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.023484753681224, p99 latency 30, number of workers: 20, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.843260733421474, p99 latency 31, number of workers: 24, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.828450511131642, p99 latency 39, number of workers: 28, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.490030494956603, p99 latency 32, number of workers: 32, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.326482951891638, p99 latency 61, number of workers: 36, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.64691295759257, p99 latency 39, number of workers: 40, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.99243515158497, p99 latency 39, number of workers: 44, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.258401492823634, p99 latency 39, number of workers: 48, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
21.049219552929085, p99 latency 42, number of workers: 52, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.38604775879277, p99 latency 42, number of workers: 56, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.648520775931647, p99 latency 46, number of workers: 60, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
24.63169677926159, p99 latency 51, number of workers: 64, dprType: v2, depProb: 0.25, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
10.106405174212393, p99 latency 42, number of workers: 4, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.223855698050256, p99 latency 31, number of workers: 8, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.26065197747967, p99 latency 32, number of workers: 12, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.214203775159037, p99 latency 31, number of workers: 16, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.19575461862463, p99 latency 31, number of workers: 20, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.627514304770596, p99 latency 129, number of workers: 24, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.284705672068636, p99 latency 32, number of workers: 28, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.0873420409897, p99 latency 117, number of workers: 32, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.866966730082131, p99 latency 31, number of workers: 36, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.205349133165043, p99 latency 35, number of workers: 40, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.767742057775418, p99 latency 35, number of workers: 44, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.62955479690657, p99 latency 38, number of workers: 48, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
22.908111838726246, p99 latency 46, number of workers: 52, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.582061097350447, p99 latency 42, number of workers: 56, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
23.252766087901676, p99 latency 44, number of workers: 60, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
27.859065855937597, p99 latency 117, number of workers: 64, dprType: v2, depProb: 0.5, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.305631149256856, p99 latency 54, number of workers: 4, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
7.241242702251877, p99 latency 29, number of workers: 8, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
9.786836252432582, p99 latency 30, number of workers: 12, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.075908451071372, p99 latency 31, number of workers: 16, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.5429941618015, p99 latency 31, number of workers: 20, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.498904919172606, p99 latency 32, number of workers: 24, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.830283704851592, p99 latency 30, number of workers: 28, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
20.52235000398396, p99 latency 130, number of workers: 32, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.052802298371216, p99 latency 34, number of workers: 36, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.390033790830588, p99 latency 38, number of workers: 40, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.521153261365363, p99 latency 38, number of workers: 44, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.09991643163063, p99 latency 41, number of workers: 48, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
23.117280480557653, p99 latency 43, number of workers: 52, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.68998405391582, p99 latency 40, number of workers: 56, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
24.538208389112004, p99 latency 48, number of workers: 60, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
38.92815172494037, p99 latency 156, number of workers: 64, dprType: v2, depProb: 0.75, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
17.16464941569282, p99 latency 45, number of workers: 4, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
13.009383797309978, p99 latency 33, number of workers: 8, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.569143016138009, p99 latency 31, number of workers: 12, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
11.339345287739784, p99 latency 31, number of workers: 16, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
16.00237698081735, p99 latency 30, number of workers: 20, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.766640019464043, p99 latency 31, number of workers: 24, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
12.269867746931967, p99 latency 29, number of workers: 28, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
18.848721214050727, p99 latency 113, number of workers: 32, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.531576872231371, p99 latency 33, number of workers: 36, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.127449548177056, p99 latency 35, number of workers: 40, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
14.07562372033063, p99 latency 34, number of workers: 44, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
15.303843881416606, p99 latency 37, number of workers: 48, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.934143088960653, p99 latency 43, number of workers: 52, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.92593089467251, p99 latency 42, number of workers: 56, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
19.96023723999666, p99 latency 46, number of workers: 60, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25										
36.88309708195583, p99 latency 152, number of workers: 64, dprType: v2, depProb: 1, heavyHitterProb: 1, delayProb: 0, averageMilli: 25									
*/
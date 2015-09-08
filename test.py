class Solution:
    def combinationSum3(self, k, n):
        import copy
        s = {}
        ss = [1,2,3,4,5,6,7,8,9]
        for i in xrange(1,10):
            s[1] = False
        ans = []
        def dfs(sums, l, k, n, ss):
            for e in ss:
                #print e
                if e > l and sum(sums)+e==n and len(sums) == k-1:
                    sums.append(e)
                    ans.append(sums)
                elif e > l and len(sums) < k:
                    newsums = copy.deepcopy(sums)
                    newsums.append(e)
                    #print newsums
                    dfs(newsums, e, k, n, ss)
                else:
                    pass
        dfs([], 0, k, n, ss)
        return ans

    def shortestPalindrome2(self, s):
        def isPalindrome(s):
            l = len(s)
            if l % 2 == 1:
                mid = l / 2
                i = mid
                j = mid
                while i-1 >= 0 and j+1 <= l-1:
                    if s[i-1] == s[j+1]:
                        i -= 1
                        j += 1
                    else:
                        return False
                return True
            if l % 2 == 0:
                mid = l / 2
                i = mid - 1
                j = mid
                if s[i] == s[j]:
                    while i-1 >= 0 and j+1 <= l-1:
                        if s[i-1] == s[j+1]:
                            i -= 1
                            j += 1
                        else:
                            return False
                else:
                    return False
                return True
        l = len(s)         
        length = 0           
        pal = ""       
        #for i in xrange(l):
        j = l
        while j > 0:
            #print s[:i+1], isPalindrome(s[:i+1])
            if j > length and isPalindrome(s[:j]):
                print "XD"
                length = j
                pal = s[:j]
                break
            j -= 1
        #print length, pal, s[length:]
        for i in xrange(length,l):
            pal = s[i] + pal
        pal += s[length:]
        return pal

    def shortestPalindrome(self, s):
        revs = s
        revs = revs[::-1]
        l = s + "#" + revs
        p = [0 for i in xrange(len(l))]
        for i in xrange(1,len(l)):
            j = p[i-1]
            while j > 0 and l[i] != l[j]:
                j = p[j-1]
            j += (l[i] == l[j])
            p[i] = j
        return revs[:len(s)-p[len(l)-1]] + s

    def maximalSquare(self, matrix):
        if not matrix:
            return 0
        n = len(matrix)
        m = len(matrix[0])
        dp = [[0 for i in xrange(m)] for i in xrange(n)]
        for i in xrange(m):
            dp[0][i] = matrix[0][i]
        for i in xrange(n):
            dp[i][0] = matrix[i][0]
        for i in xrange(1,n):
            for j in xrange(1,m):
                pass
        return dp[n-1][m-1]
    def maximalSquare(self, matrix):
        n = len(matrix)
        m = len(matrix[0])
        if n==0 or m==0:
            return 0
        maxs = 0
        dp = [[0 for i in xrange(m)] for i in xrange(n)]
        for i in xrange(m):
            dp[0][i] = int(matrix[0][i])
            maxs = max(maxs, int(matrix[0][i]))
        for i in xrange(n):
            dp[i][0] = int(matrix[i][0])
            maxs = max(maxs, int(matrix[i][0]))
        for i in xrange(1,n):
            for j in xrange(1,m):
                if matrix[i][j] == "0":
                    dp[i][j] = 0
                else:
                    dp[i][j] = min(dp[i-1][j-1], dp[i-1][j], dp[i][j-1]) + 1
            for j in xrange(m):
                maxs = max(maxs, dp[i][j])
        return maxs**2
        
    def reverse(self, x):
        num = x
        tmp = 0
        overflow  = pow(2,31)-1
        while num > 0:
            dig = num % 10
            num /= 10
            tmp = tmp * 10 + dig
            if tmp > overflow:
                return 0
        return tmp

    def maximumGap(self, nums):
        l = len(nums)
        if l < 2:
            return 0
        if l == 2:
            return abs(nums[0]-nums[1])
        nmax = max(nums)
        nmin = min(nums)
        diff = nmax - nmin
        step = diff / (l - 1) if diff % (l-1) == 0 else diff / (l-1) + 1
        buck = [[pow(2,31), -1] for i in xrange(l)]
        def determine(tup):
            if tup[0] == pow(2,31) and tup[1] == -1:
                return False
            return True
        for i in xrange(l):
            bucknum = (nums[i] - nmin) / step if (nums[i] - nmin) % step == 0 else (nums[i] - nmin) / step + 1
            buck[bucknum][0] = min(nums[i], buck[bucknum][0])
            buck[bucknum][1] = max(nums[i], buck[bucknum][1])
        buck[:] = [tup for tup in buck if determine(tup)]
        maxdiff = 0
        maxdiff = max(maxdiff, buck[0][1] - buck[0][0])
        for i in xrange(1,len(buck)):
            maxdiff = max(maxdiff, buck[i][1] - buck[i][0])
            maxdiff = max(maxdiff, buck[i][0] - buck[i-1][1])
        print maxdiff
        return maxdiff

sc = Solution()
sol = sc.combinationSum3(3,9)
sol = sc.shortestPalindrome("abcd")
sol = sc.maximalSquare(["11","11"])
sol = sc.reverse(1534236469)
sol = sc.maximumGap([100,3,2,1])
#N = "1234"
#l = len(N)
#dp = [0 for i in xrange(l+1)]
#mod = 10**9 + 7
#dp[1] = int(N[0])
#dp[2] = int(N[0])+int(N[1])+int(N[0:2])
#for i in xrange(3,l+1):
#    dp[i] = dp[i-1]*11 - dp[i-2]*10 + int(N[i-1])*i
#print dp[l]%mod